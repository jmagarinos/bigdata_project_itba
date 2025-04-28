# scripts/funnel_plot.py

import os
import matplotlib.pyplot as plt
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Crear sesión de Spark simple
spark = SparkSession.builder.appName("FunnelPlotOnboarding").getOrCreate()

# Paths
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
DATA_FINAL_PATH = os.path.join(PROJECT_ROOT, 'data', 'processed', 'clean', 'final')

# Leer parquet final
df = spark.read.parquet(os.path.join(DATA_FINAL_PATH, 'onboarding_final.parquet'))

# Renombrar 'drop' a 'drop_flag' si no lo hiciste ya
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

# Plot
plt.figure(figsize=(8,6))
plt.plot(funnel_df["Etapa"], funnel_df["Porcentaje"], marker="o")
plt.gca().invert_yaxis()
plt.title("Funnel de Onboarding")
plt.ylabel("Porcentaje de Usuarios (%)")
plt.grid(True)
plt.show()
