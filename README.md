# bigdata_project_itba

bigdata_project_itba/
├── config/
│   ├── __init__.py
│   └── spark_session.py               # Configura Spark + Cassandra Connector
├── scripts/
│   ├── ingestion.py                   # Carga CSV a Parquet
│   ├── preprocessing.py               # Limpieza de datos
│   ├── transformation.py              # Cálculo de métricas (drop, activación, hábito, setup)
│   ├── cassandra_load.py               # Carga final en Cassandra
│   ├── check_cassandra.py              # Script para verificar datos en Cassandra
│   ├── funnel_plot.py                  # Gráfico de funnel de onboarding (opcional para presentación)
├── data/
│   ├── raw/                            # CSV originales (lk_users.csv, bt_users_transactions.csv, etc.)
│   ├── processed/
│   │   ├── lk_users.parquet
│   │   ├── bt_users_transactions.parquet
│   │   ├── lk_onboarding.parquet
│   │   └── clean/
│   │       ├── lk_users_clean.parquet
│   │       ├── bt_users_transactions_clean.parquet
│   │       ├── lk_onboarding_clean.parquet
│   │       └── final/
│   │           └── onboarding_final.parquet
├── diagrams/
│   └── architecture_diagram.png       # Diagrama de arquitectura Big Data
├── notebooks/
│   ├── exploratory_analysis.ipynb     # Análisis exploratorio inicial (opcional)
│   ├── pruebas_funnel.ipynb            # Notebook para el gráfico de funnel (opcional)
├── README.md                           # Documentación general del proyecto
├── .gitignore                          # Archivos/carpetas que no se versionan
└── requirements.txt                    # Librerías necesarias (pyspark, cassandra-driver, matplotlib, pandas)
