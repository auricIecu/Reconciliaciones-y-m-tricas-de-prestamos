# 📘 ETL de Conciliación y Análisis de Riesgo de Cartera  
**Versión avanzada – Proyecto demostrativo para entrevistas (Riesgo, Datos, Control, Capital)**

Este proyecto implementa un pipeline ETL completo para conciliación de datos financieros, detección de discrepancias, validación de calidad de datos y análisis de riesgo a nivel de cartera. Está diseñado para demostrar competencias técnicas y de negocio en áreas como **crédito, riesgo, contabilidad, operaciones, automatización y reporting regulatorio**.

## 📂 Estructura del proyecto

etl_reconciliation_project/
│
├── data/
│   ├── originacion.csv
│   ├── contabilidad.csv
│   └── riesgo.csv
│
├── output/
│   ├── reconciliation_report.csv
│   ├── summary_metrics.csv
│   ├── data_quality_report.csv
│   ├── kpis.csv / kpis.json
│   ├── concentration_by_center.csv
│   ├── concentration_by_risk_segment.csv
│   ├── concentration_by_pd_bucket.csv
│   ├── mismatch_by_center.png
│
├── etl.py
└── requirements.txt

## 🚀 ¿Qué hace el pipeline?

1. Carga datos desde los tres sistemas.
2. Reconciliación entre originación y contabilidad.
3. Enriquecimiento con riesgo.
4. Clasificación PD en buckets.
5. Cálculo de Risk_Mismatch_Score.
6. Data Quality checks completos.
7. Detección de anomalías por percentil.
8. KPIs globales del portafolio.
9. Análisis de concentración.
10. Visualización final.

## 🛠 Ejecución

pip install -r requirements.txt  
python etl.py

Los resultados aparecen en la carpeta output/.

