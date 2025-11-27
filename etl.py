"""
ETL pipeline for credit portfolio reconciliation (versión extendida + gráficos seleccionados)
--------------------------------------------------------------------------------------------

Este script:

1. Carga datos de originación, contabilidad y riesgo desde la carpeta `data/`.
2. Reconciliación originación ↔ contabilidad:
   - Calcula diferencias entre ImporteAprobado y SaldoContable.
   - Marca préstamos con discrepancias según un umbral de tolerancia.
3. Enriquecer con métricas de riesgo (PD, LGD, EAD, Provision).
4. Añade:
   - Análisis de concentración de discrepancias.
   - Risk_Mismatch_Score.
   - Data Quality checks.
   - Detección de anomalías.
   - KPIs agregados.
5. Genera informes en la carpeta `output/`:
   - Informes detallados y agregados (CSV).
   - Informes de calidad de datos.
   - Informes de concentración.
   - KPIs en CSV y JSON.
   - Gráficos PNG de:
      * mismatches por centro de coste
      * resumen mismatch/no mismatch por centro
      * mismatches por segmento de riesgo
      * mismatches por bucket de PD

Requisitos:
- Python 3.8+
- pandas
- matplotlib
"""

from __future__ import annotations

import json
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd


# -----------------------------------------------------------------------------
# 1. Carga de datos
# -----------------------------------------------------------------------------
def load_data(data_dir: Path) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Carga los CSV de originación, contabilidad y riesgo desde data_dir."""
    originacion = pd.read_csv(
        data_dir / "originacion.csv",
        parse_dates=["FechaDesembolso"],
    )

    contabilidad = pd.read_csv(
        data_dir / "contabilidad.csv",
        parse_dates=["Fecha"],
    )

    riesgo = pd.read_csv(data_dir / "riesgo.csv")

    return originacion, contabilidad, riesgo


# -----------------------------------------------------------------------------
# 2. Reconciliación originación ↔ contabilidad
# -----------------------------------------------------------------------------
def reconcile(
    originacion: pd.DataFrame,
    contabilidad: pd.DataFrame,
    tolerance: float = 1000.0,
) -> pd.DataFrame:
    """
    Realiza un full outer join entre originación y contabilidad por LoanID y ClienteID.
    Calcula diferencias, bandera de mismatches y deja listo para enriquecer con riesgo.
    """
    merged = originacion.merge(
        contabilidad,
        on=["LoanID", "ClienteID"],
        how="outer",
        suffixes=("_orig", "_cont"),
        indicator=True,  # columna _merge: 'left_only', 'right_only', 'both'
    )

    # Diferencias entre ImporteAprobado y SaldoContable
    merged["ImporteAprobado"] = merged["ImporteAprobado"].fillna(0)
    merged["SaldoContable"] = merged["SaldoContable"].fillna(0)

    merged["difference"] = merged["ImporteAprobado"] - merged["SaldoContable"]
    merged["abs_difference"] = merged["difference"].abs()

    # Flag de mismatch por tolerancia
    merged["mismatch"] = merged["abs_difference"] > tolerance

    return merged


# -----------------------------------------------------------------------------
# 3. Enriquecer con riesgo
# -----------------------------------------------------------------------------
def merge_risk(reconciled: pd.DataFrame, riesgo: pd.DataFrame) -> pd.DataFrame:
    """
    Hace un left join de la tabla reconciliada con la tabla de riesgo usando LoanID.
    """
    enriched = reconciled.merge(riesgo, on="LoanID", how="left")
    return enriched


# -----------------------------------------------------------------------------
# 4. Análisis de concentración (CentroCosto, RiesgoAsignado, buckets de PD)
# -----------------------------------------------------------------------------
def add_pd_bucket(enriched: pd.DataFrame) -> pd.DataFrame:
    """
    Crea un bucket de PD para análisis de concentración.
    """
    bins = [0, 0.01, 0.03, 0.07, 1.0]
    labels = ["[0-1%]", "(1-3%]", "(3-7%]", "(>7%)"]
    enriched["PD_bucket"] = pd.cut(
        enriched["PD"].fillna(0), bins=bins, labels=labels, include_lowest=True
    )
    return enriched


def build_concentration_reports(
    enriched: pd.DataFrame, output_dir: Path
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Genera varios informes de concentración:
    - Por CentroCosto y mismatch.
    - Por RiesgoAsignado y mismatch.
    - Por PD_bucket y mismatch.

    Devuelve los tres DataFrames agregados para poder graficarlos.
    """
    # Nos aseguramos de que exista PD_bucket
    if "PD_bucket" not in enriched.columns:
        enriched = add_pd_bucket(enriched)

    # Concentración por centro de coste
    conc_center = (
        enriched.groupby(["CentroCosto", "mismatch"], dropna=False)
        .agg(
            num_loans=("LoanID", "count"),
            total_abs_diff=("abs_difference", "sum"),
        )
        .reset_index()
    )
    conc_center.to_csv(output_dir / "concentration_by_center.csv", index=False)

    # Concentración por riesgo asignado
    conc_risk = (
        enriched.groupby(["RiesgoAsignado", "mismatch"], dropna=False)
        .agg(
            num_loans=("LoanID", "count"),
            total_abs_diff=("abs_difference", "sum"),
        )
        .reset_index()
    )
    conc_risk.to_csv(output_dir / "concentration_by_risk_segment.csv", index=False)

    # Concentración por bucket de PD
    conc_pd = (
        enriched.groupby(["PD_bucket", "mismatch"], dropna=False)
        .agg(
            num_loans=("LoanID", "count"),
            total_abs_diff=("abs_difference", "sum"),
        )
        .reset_index()
    )
    conc_pd.to_csv(output_dir / "concentration_by_pd_bucket.csv", index=False)

    return conc_center, conc_risk, conc_pd


# -----------------------------------------------------------------------------
# 5. Risk_Mismatch_Score
# -----------------------------------------------------------------------------
def add_risk_mismatch_score(enriched: pd.DataFrame) -> pd.DataFrame:
    """
    Añade una columna Risk_Mismatch_Score = PD * LGD * abs_difference.
    Sólo tiene sentido cuando PD, LGD y abs_difference no son nulos.
    """
    pd_series = enriched["PD"].fillna(0)
    lgd_series = enriched["LGD"].fillna(0)
    abs_diff = enriched["abs_difference"].fillna(0)

    enriched["Risk_Mismatch_Score"] = pd_series * lgd_series * abs_diff

    return enriched


# -----------------------------------------------------------------------------
# 6. Data Quality checks
# -----------------------------------------------------------------------------
def run_data_quality_checks(enriched: pd.DataFrame) -> pd.DataFrame:
    """
    Ejecuta una serie de controles de calidad de datos y devuelve un DataFrame
    con un registro por LoanID y flags de calidad.
    """
    dq = pd.DataFrame()
    dq["LoanID"] = enriched["LoanID"]

    dq["missing_PD"] = enriched["PD"].isna()
    dq["missing_LGD"] = enriched["LGD"].isna()
    dq["missing_EAD"] = enriched["EAD"].isna()
    dq["negative_saldo"] = enriched["SaldoContable"] < 0
    dq["negative_importe_aprobado"] = enriched["ImporteAprobado"] < 0
    dq["inconsistent_provision"] = (enriched["Provision"] < 0) | enriched[
        "Provision"
    ].isna()
    dq["missing_centro_costo"] = enriched["CentroCosto"].isna()

    problem_cols = [
        "missing_PD",
        "missing_LGD",
        "missing_EAD",
        "negative_saldo",
        "negative_importe_aprobado",
        "inconsistent_provision",
        "missing_centro_costo",
    ]
    dq["any_quality_issue"] = dq[problem_cols].any(axis=1)

    return dq


# -----------------------------------------------------------------------------
# 7. Detección de anomalías (basada en percentil de abs_difference)
# -----------------------------------------------------------------------------
def detect_anomalies(enriched: pd.DataFrame, quantile: float = 0.99) -> pd.DataFrame:
    """
    Marca como 'anomaly' aquellas filas cuya abs_difference sea superior al
    percentil indicado (por defecto 99%).
    """
    if enriched["abs_difference"].notna().sum() == 0:
        enriched["anomaly"] = False
        return enriched

    threshold = enriched["abs_difference"].quantile(quantile)
    enriched["anomaly"] = enriched["abs_difference"] > threshold
    return enriched


# -----------------------------------------------------------------------------
# 8. Cálculo de KPIs
# -----------------------------------------------------------------------------
def compute_kpis(enriched: pd.DataFrame, dq_report: pd.DataFrame) -> dict:
    """
    Calcula KPIs agregados a nivel de portafolio.
    Devuelve un dict serializable a JSON.
    """
    total_loans = enriched["LoanID"].nunique()
    mismatches = enriched["mismatch"].sum()
    anomalies = enriched["anomaly"].sum() if "anomaly" in enriched.columns else 0

    total_approved = enriched["ImporteAprobado"].sum()
    total_ledger = enriched["SaldoContable"].sum()
    total_abs_diff = enriched["abs_difference"].sum()

    dq_issues = dq_report["any_quality_issue"].sum()

    kpis = {
        "total_loans": int(total_loans),
        "num_mismatches": int(mismatches),
        "pct_mismatches": float(mismatches / total_loans) if total_loans else 0.0,
        "num_anomalies": int(anomalies),
        "total_approved": float(total_approved),
        "total_ledger": float(total_ledger),
        "total_abs_difference": float(total_abs_diff),
        "num_quality_issues": int(dq_issues),
        "pct_quality_issues": float(dq_issues / total_loans) if total_loans else 0.0,
    }

    return kpis


# -----------------------------------------------------------------------------
# 9. Gráficos auxiliares (solo los que quieres)
# -----------------------------------------------------------------------------
def plot_summary_charts(summary: pd.DataFrame, output_dir: Path) -> None:
    """
    Genera gráficos a partir de summary_metrics.csv:
    - Barras por centro de coste comparando mismatch True vs False.
    """
    if summary.empty:
        return

    pivot = summary.pivot_table(
        index="CentroCosto",
        columns="mismatch",
        values="num_loans",
        aggfunc="sum",
        fill_value=0,
    )

    plt.figure(figsize=(8, 4))
    pivot.plot(kind="bar", stacked=True)
    plt.title("Número de préstamos por Centro de Costo (mismatch vs no mismatch)")
    plt.xlabel("Centro de Costo")
    plt.ylabel("Número de préstamos")
    plt.tight_layout()
    plt.savefig(output_dir / "summary_mismatch_by_center.png")
    plt.close()


def plot_concentration_charts(
    conc_center: pd.DataFrame,
    conc_risk: pd.DataFrame,
    conc_pd: pd.DataFrame,
    output_dir: Path,
) -> None:
    """
    Genera gráficos a partir de los informes de concentración:
    - mismatch_by_center.png (desde enriched, en otra función).
    - mismatch_by_risk_segment.png.
    - mismatch_by_pd_bucket.png.
    """
    # Gráfico por RiesgoAsignado
    if not conc_risk.empty:
        mism = conc_risk[conc_risk["mismatch"] == True]
        if not mism.empty:
            plt.figure(figsize=(6, 4))
            mism.set_index("RiesgoAsignado")["num_loans"].plot(kind="bar")
            plt.title("Préstamos con mismatch por segmento de riesgo")
            plt.xlabel("Riesgo Asignado")
            plt.ylabel("Número de préstamos")
            plt.tight_layout()
            plt.savefig(output_dir / "mismatch_by_risk_segment.png")
            plt.close()

    # Gráfico por PD_bucket
    if not conc_pd.empty:
        mism_pd = conc_pd[conc_pd["mismatch"] == True]
        if not mism_pd.empty:
            plt.figure(figsize=(6, 4))
            mism_pd.set_index("PD_bucket")["num_loans"].plot(kind="bar")
            plt.title("Préstamos con mismatch por bucket de PD")
            plt.xlabel("Bucket de PD")
            plt.ylabel("Número de préstamos")
            plt.tight_layout()
            plt.savefig(output_dir / "mismatch_by_pd_bucket.png")
            plt.close()


# -----------------------------------------------------------------------------
# 10. Guardado de informes
# -----------------------------------------------------------------------------
def save_reports(
    enriched: pd.DataFrame,
    dq_report: pd.DataFrame,
    kpis: dict,
    output_dir: Path,
) -> None:
    """
    Guarda:
    - Informe detallado de conciliación.
    - Informe agregado por centro de coste y mismatch.
    - Data quality report.
    - KPIs en CSV y JSON.
    - Concentration reports.
    - Gráficos:
        * mismatch_by_center.png
        * summary_mismatch_by_center.png
        * mismatch_by_risk_segment.png
        * mismatch_by_pd_bucket.png
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    # 10.1 Informe detallado
    detailed_cols = [
        "LoanID",
        "ClienteID",
        "FechaDesembolso",
        "ImporteAprobado",
        "Fecha",
        "SaldoContable",
        "difference",
        "abs_difference",
        "mismatch",
        "anomaly",
        "Risk_Mismatch_Score",
        "RiesgoAsignado",
        "PD",
        "LGD",
        "EAD",
        "Provision",
        "CentroCosto",
        "PD_bucket",
        "_merge",
    ]
    existing_detailed_cols = [c for c in detailed_cols if c in enriched.columns]
    detailed = enriched[existing_detailed_cols].copy()
    detailed.to_csv(output_dir / "reconciliation_report.csv", index=False)

    # 10.2 Informe agregado (resumen por centro y mismatch)
    summary = (
        enriched.groupby(["CentroCosto", "mismatch"], dropna=False)
        .agg(
            num_loans=("LoanID", "count"),
            total_approved=("ImporteAprobado", "sum"),
            total_ledger=("SaldoContable", "sum"),
            total_abs_difference=("abs_difference", "sum"),
        )
        .reset_index()
    )
    summary.to_csv(output_dir / "summary_metrics.csv", index=False)

    # 10.3 Data Quality report
    dq_report.to_csv(output_dir / "data_quality_report.csv", index=False)

    # 10.4 KPIs en JSON y CSV
    (output_dir / "kpis.json").write_text(json.dumps(kpis, indent=2), encoding="utf-8")
    pd.DataFrame([kpis]).to_csv(output_dir / "kpis.csv", index=False)

    # 10.5 Informes de concentración (y recuperar DataFrames para gráficos)
    conc_center, conc_risk, conc_pd = build_concentration_reports(
        enriched, output_dir=output_dir
    )

    # 10.6 Gráfico principal de mismatches por centro de coste (a partir de enriched)
    mismatch_counts = (
        enriched[enriched["mismatch"]]
        .groupby("CentroCosto")["LoanID"]
        .count()
        .sort_values()
    )

    if not mismatch_counts.empty:
        plt.figure(figsize=(8, 4))
        mismatch_counts.plot(kind="barh")
        plt.title("Número de préstamos con discrepancias por Centro de Costo")
        plt.xlabel("Número de préstamos con discrepancias")
        plt.ylabel("Centro de Costo")
        plt.tight_layout()
        plt.savefig(output_dir / "mismatch_by_center.png")
        plt.close()

    # 10.7 Gráfico resumen mismatch/no mismatch por centro
    plot_summary_charts(summary, output_dir)

    # 10.8 Gráficos de concentración por riesgo y por bucket de PD
    plot_concentration_charts(conc_center, conc_risk, conc_pd, output_dir)


# -----------------------------------------------------------------------------
# 11. main()
# -----------------------------------------------------------------------------
def main() -> None:
    project_path = Path(__file__).parent
    data_dir = project_path / "data"
    output_dir = project_path / "output"

    # Cargar datos
    originacion, contabilidad, riesgo = load_data(data_dir)

    # Reconciliación
    reconciled = reconcile(originacion, contabilidad, tolerance=1000.0)

    # Enriquecer con riesgo
    enriched = merge_risk(reconciled, riesgo)

    # Añadir PD buckets
    enriched = add_pd_bucket(enriched)

    # Añadir Risk_Mismatch_Score
    enriched = add_risk_mismatch_score(enriched)

    # Detectar anomalías
    enriched = detect_anomalies(enriched, quantile=0.99)

    # Data Quality checks
    dq_report = run_data_quality_checks(enriched)

    # KPIs
    kpis = compute_kpis(enriched, dq_report)

    # Guardar informes y gráficos
    save_reports(enriched, dq_report, kpis, output_dir)

    print(f"Reconciliation completed. Reports and charts saved to {output_dir}")


if __name__ == "__main__":
    main()
