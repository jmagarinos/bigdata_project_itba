# bigdata_project_itba

📚 Descripción del proyecto
Este proyecto implementa un pipeline de Ingeniería de Datos sobre una arquitectura de Big Data para una fintech de Latinoamérica que busca reducir el abandono de usuarios en su aplicación.
El proceso completo de ETL fue desarrollado utilizando Apache Spark y Apache Cassandra, y aborda la medición de métricas de onboarding como drop, activación, hábito y setup.

## 🖼️ Arquitectura Big Data

![Diagrama de Arquitectura](diagrams/BigData_Architecture_Diagram.png)

## 🛠️ Estructura del proyecto

```plaintext
bigdata-onboarding-fintech/
├── config/
│   ├── __init__.py
│   └── spark_session.py               # Configuración de Spark + Cassandra Connector
├── scripts/
│   ├── ingestion.py                   # Ingesta de CSV a Parquet
│   ├── preprocessing.py               # Limpieza de datos
│   ├── transformation.py              # Cálculo de métricas de negocio
│   ├── cassandra_load.py               # Carga de datos finales en Cassandra
│   ├── check_cassandra.py              # Consulta de datos en Cassandra
│   ├── funnel_plot.py                  # Gráfico de funnel de onboarding
├── data/
│   ├── bronze/                         # Datos crudos y parquet de ingesta
│   ├── silver/                         # Datos limpios
│   └── gold/                           # Métricas finales para consumo
├── diagrams/
│   └── architecture_diagram.png        # Diagrama de arquitectura Big Data
├── notebooks/
│   ├── exploratory_analysis.ipynb       # Análisis exploratorio (opcional)
│   ├── pruebas_funnel.ipynb              # Notebook para el gráfico de funnel (opcional)
├── README.md                             # Documentación general
├── .gitignore                            # Archivos ignorados en el repo
└── requirements.txt                      # Dependencias necesarias

```

El flujo de datos sigue un esquema *Bronze ➔ Silver ➔ Gold* donde:

- **Bronze** almacena los datos crudos y los Parquet generados en la ingesta.
- **Silver** contiene la información ya depurada lista para transformaciones.
- **Gold** aloja las métricas finales listas para ser consumidas o cargadas en Cassandra.

## 🚀 Cómo ejecutar el proyecto

1) Instalar dependencias
Instalar las librerías necesarias:

```bash
pip install -r requirements.txt
```

2) Correr los scripts en orden
Ingesta de datos CSV ➔ Parquet:

```bash
python3.11 -m scripts.ingestion
```

3) Preprocesamiento (limpieza de datos):

```bash
python3.11 -m scripts.preprocessing
```

4) Transformación de métricas de onboarding:

```bash
python3.11 -m scripts.transformation
```

5) Carga en Cassandra (asegurarse que Cassandra esté corriendo):

```bash
python3.11 -m scripts.cassandra_load
```

6) Verificación de datos en Cassandra:

```bash
python3.11 -m scripts.check_cassandra
```

7) Visualización del funnel de onboarding:

```bash
python3.11 -m scripts.funnel_plot
```

## 🧱 Requerimientos técnicos

Python 3.11

Apache Spark 3.4.1

Apache Cassandra 4.1 (corriendo en localhost)

Conector spark-cassandra-connector

Bibliotecas Python:

- pyspark

- cassandra-driver

- pandas

- matplotlib
