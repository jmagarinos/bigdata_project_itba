# scripts/funnel_plot.py

import os
import matplotlib.pyplot as plt
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Crear sesión de Spark
spark = SparkSession.builder.appName("FunnelPlotOnboarding").getOrCreate()

# Paths
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

# Capa Gold
DATA_FINAL_PATH = os.path.join(PROJECT_ROOT, 'data', 'gold')

# Leer parquet final
df = spark.read.parquet(os.path.join(DATA_FINAL_PATH, 'onboarding_final.parquet'))

# Renombrar 'drop' a 'drop_flag'
df = df.withColumnRenamed("drop", "drop_flag")

# Cálculos
total_users = df.count()

non_dropped = df.filter(col("drop_flag") == 0).count()
activated = df.filter(col("activacion") == 1).count()
habitual = df.filter(col("habito_final") == 1).count()
setup_done = df.filter(col("setup") == 1).count()

# Porcentajes
funnel_data = {
    "Etapa": ["No Drop", "Activados", "Hábito", "Setup"],
    "Porcentaje": [
        non_dropped / total_users * 100,
        activated / total_users * 100,
        habitual / total_users * 100,
        setup_done / total_users * 100
    ]
}

funnel_df = pd.DataFrame(funnel_data)

# Funnel plot con barras horizontales
plt.figure(figsize=(8, 6))
plt.barh(funnel_df["Etapa"], funnel_df["Porcentaje"], color="skyblue", edgecolor="black")
plt.gca().invert_yaxis()  # Poner "No Drop" arriba y "Setup" abajo
plt.title("Funnel de Onboarding")
plt.xlabel("Porcentaje de Usuarios (%)")
plt.grid(axis="x", linestyle="--", alpha=0.7)
plt.tight_layout()
plt.show()
