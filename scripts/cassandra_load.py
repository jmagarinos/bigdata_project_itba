# scripts/cassandra_load.py

import os
from config.spark_session import get_spark_session
from pyspark.sql.functions import col

# Definir paths
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

# Capa Gold
DATA_FINAL_PATH = os.path.join(PROJECT_ROOT, 'data', 'gold')

def main():
    # Crear sesión de Spark
    spark = get_spark_session("CassandraLoadOnboardingFintech")

    # Leer dataset final
    onboarding_final = spark.read.parquet(os.path.join(DATA_FINAL_PATH, 'onboarding_final.parquet'))

    # Renombrar columna 'drop' a 'drop_flag' para Cassandra
    onboarding_final = onboarding_final.withColumnRenamed("drop", "drop_flag")

    # Seleccionar solo las columnas que existen en la tabla Cassandra
    onboarding_final = onboarding_final.select(
        "user_id",
        "drop_flag",
        "activacion",
        "setup",
        "habito_final",
        "first_login_dt"
    )
    
    # Configurar opciones de conexión a Cassandra
    onboarding_final.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .options(table="users_metrics", keyspace="fintech_onboarding") \
        .save()

    print("\n✅ Datos cargados en Cassandra (tabla users_metrics en keyspace fintech_onboarding)")

if __name__ == "__main__":
    main()
