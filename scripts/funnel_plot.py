# scripts/funnel_plot.py

import os
import matplotlib.pyplot as plt
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Crear sesión de Spark
spark = SparkSession.builder \
    .appName("FunnelPlotOnboarding") \
    .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1") \
    .config("spark.cassandra.connection.host", "localhost") \
    .getOrCreate()

# Leer datos desde Cassandra
df = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="users_metrics", keyspace="fintech_onboarding") \
    .load()

# Renombrar 'drop' si es necesario (si tu columna en Cassandra se llama 'drop_flag', quita esta línea)
df = df.withColumnRenamed("drop", "drop_flag")

# Cálculos de conteos
total_users  = df.count()
non_dropped  = df.filter(col("drop_flag") == 0).count()
setup_done   = df.filter(col("setup") == 1).count()
activated    = df.filter(col("activacion") == 1).count()
habitual     = df.filter(col("habito_final") == 1).count()

# Porcentajes en el orden correcto: No Drop → Setup → Activados → Hábito
funnel_data = {
    "Etapa": ["No Drop", "Setup", "Activados", "Hábito"],
    "Porcentaje": [
        non_dropped / total_users * 100,
        setup_done  / total_users * 100,
        activated   / total_users * 100,
        habitual    / total_users * 100,
    ]
}

funnel_df = pd.DataFrame(funnel_data)

# Plot del funnel
plt.figure(figsize=(8, 6))
plt.barh(funnel_df["Etapa"], funnel_df["Porcentaje"], color="skyblue", edgecolor="black")
plt.gca().invert_yaxis()  # invierte para que "No Drop" quede arriba
plt.title("Funnel de Onboarding (Cassandra)")
plt.xlabel("Porcentaje de Usuarios (%)")
plt.grid(axis="x", linestyle="--", alpha=0.7)
plt.tight_layout()
plt.show()
