# scripts/preprocessing.py

import os
from pyspark.sql.functions import col, to_date
from pyspark.sql.functions import to_timestamp
from pyspark.sql.types import IntegerType
from config.spark_session import get_spark_session

# Definir la raíz del proyecto
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
DATA_PROCESSED_PATH = os.path.join(PROJECT_ROOT, 'data', 'processed')

def main():
    # Crear sesión de Spark
    spark = get_spark_session("PreprocessingOnboardingFintech")

    # Leer datasets procesados
    lk_users = spark.read.parquet(os.path.join(DATA_PROCESSED_PATH, 'lk_users.parquet'))
    bt_users_transactions = spark.read.parquet(os.path.join(DATA_PROCESSED_PATH, 'bt_users_transactions.parquet'))
    lk_onboarding = spark.read.parquet(os.path.join(DATA_PROCESSED_PATH, 'lk_onboarding.parquet'))

    # --- LIMPIEZA DE DATOS ---

    # 1. Eliminar columnas basura
    lk_users = lk_users.drop("_c0")
    bt_users_transactions = bt_users_transactions.drop("_c0")
    lk_onboarding = lk_onboarding.drop("_c0", "Unnamed: 0")

    # 2. Convertir columnas de fecha
    bt_users_transactions = bt_users_transactions.withColumn(
    "transaction_dt", to_timestamp(col("transaction_dt"))
    )
    
    lk_onboarding = lk_onboarding \
        .withColumn("first_login_dt", to_date(to_timestamp(col("first_login_dt")))) \
        .withColumn("habito_dt", to_date(to_timestamp(col("habito_dt")))) \
        .withColumn("activacion_dt", to_date(to_timestamp(col("activacion_dt")))) \
        .withColumn("setup_dt", to_date(to_timestamp(col("setup_dt")))) \
        .withColumn("return_dt", to_date(to_timestamp(col("return_dt"))))


    # 3. Convertir flags de string a integer (habito, activacion, setup, return)
    for col_name in ["habito", "activacion", "setup", "return"]:
        lk_onboarding = lk_onboarding.withColumn(col_name, col(col_name).cast(IntegerType()))

    # --- GUARDAR ARCHIVOS LIMPIOS ---

    output_clean_path = os.path.join(DATA_PROCESSED_PATH, 'clean')
    os.makedirs(output_clean_path, exist_ok=True)

    lk_users.write.mode("overwrite").parquet(os.path.join(output_clean_path, 'lk_users_clean.parquet'))
    bt_users_transactions.write.mode("overwrite").parquet(os.path.join(output_clean_path, 'bt_users_transactions_clean.parquet'))
    lk_onboarding.write.mode("overwrite").parquet(os.path.join(output_clean_path, 'lk_onboarding_clean.parquet'))

    print("\n✅ Preprocesamiento terminado. Archivos limpios guardados en /data/processed/clean/")

if __name__ == "__main__":
    main()
