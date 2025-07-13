# scripts/ingestion.py

import os
from config.spark_session import get_spark_session

# Configuración inicial
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
DATA_PATH = os.path.join(PROJECT_ROOT, 'data', 'raw')


def main():
    # Levantar sesión de Spark
    spark = get_spark_session("IngestionOnboardingFintech")

    # Cargar datasets
    lk_users = spark.read.option("header", "true").csv(os.path.join(DATA_PATH, 'lk_users.csv'))
    bt_users_transactions = spark.read.option("header", "true").csv(os.path.join(DATA_PATH, 'bt_users_transactions.csv'))
    lk_onboarding = spark.read.option("header", "true").csv(os.path.join(DATA_PATH, 'lk_onboarding.csv'))

    # Mostrar algunas estadísticas rápidas
    print("\n=== lk_users ===")
    lk_users.printSchema()
    print(f"Cantidad de registros: {lk_users.count()}")

    print("\n=== bt_users_transactions ===")
    bt_users_transactions.printSchema()
    print(f"Cantidad de registros: {bt_users_transactions.count()}")

    print("\n=== lk_onboarding ===")
    lk_onboarding.printSchema()
    print(f"Cantidad de registros: {lk_onboarding.count()}")

    # Opcional: guardar como parquet para usar más rápido después
    output_path = os.path.join(PROJECT_ROOT, 'data', 'processed')
    os.makedirs(output_path, exist_ok=True)

    lk_users.write.mode("overwrite").parquet(os.path.join(output_path, 'lk_users.parquet'))
    bt_users_transactions.write.mode("overwrite").parquet(os.path.join(output_path, 'bt_users_transactions.parquet'))
    lk_onboarding.write.mode("overwrite").parquet(os.path.join(output_path, 'lk_onboarding.parquet'))

    print("\nArchivos guardados en formato Parquet.")

if __name__ == "__main__":
    main()
