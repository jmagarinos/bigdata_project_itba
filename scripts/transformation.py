# scripts/transformation.py

import os
from pyspark.sql.functions import col, countDistinct, lit
from pyspark.sql import functions as F
from config.spark_session import get_spark_session

# Definir paths
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
DATA_CLEAN_PATH = os.path.join(PROJECT_ROOT, 'data', 'processed', 'clean')

def main():
    # Crear sesión de Spark
    spark = get_spark_session("TransformationOnboardingFintech")

    # Leer datasets limpios
    lk_users = spark.read.parquet(os.path.join(DATA_CLEAN_PATH, 'lk_users_clean.parquet'))
    bt_users_transactions = spark.read.parquet(os.path.join(DATA_CLEAN_PATH, 'bt_users_transactions_clean.parquet'))
    lk_onboarding = spark.read.parquet(os.path.join(DATA_CLEAN_PATH, 'lk_onboarding_clean.parquet'))

    # --- TRANSFORMACIONES ---
    
    # 1. Usuarios Brasil (user_id empieza con "MLB")
    users_brazil = lk_users.filter(col("user_id").startswith("MLB"))

    # 2. Join principal
    onboarding_full = users_brazil.join(lk_onboarding, on="user_id", how="left")

    # 3. Cálculo de métricas básicas
    onboarding_full = onboarding_full.withColumn("drop", F.when(col("return") == 0, lit(1)).otherwise(lit(0)))

    # 4. Activación y Setup ya vienen como flags en lk_onboarding_clean

    # 5. Hábito
    # Tenemos que hacer joins con transacciones:
    # Para individuals: 5 transacciones en 5 días distintos.
    # Para sellers: 5 cobros (tipo 8 o 9), sin importar días.

    # --- Hábito para individuals ---
    individual_transactions = bt_users_transactions.filter(
        (col("segment") == "1") & (col("transaction_dt").isNotNull())
    )

    habit_individuals = individual_transactions.groupBy("user_id").agg(
        countDistinct("transaction_dt").alias("days_active")
    ).withColumn("habito_individual", F.when(col("days_active") >= 5, lit(1)).otherwise(lit(0)))

    # --- Hábito para sellers ---
    seller_transactions = bt_users_transactions.filter(
        (col("segment") == "2") & (col("type").isin("8", "9")) & (col("transaction_dt").isNotNull())
    )

    habit_sellers = seller_transactions.groupBy("user_id").agg(
        F.count("*").alias("cobros_realizados")
    ).withColumn("habito_seller", F.when(col("cobros_realizados") >= 5, lit(1)).otherwise(lit(0)))

    # --- Unión de hábitos al onboarding ---
    onboarding_final = onboarding_full \
        .join(habit_individuals, on="user_id", how="left") \
        .join(habit_sellers, on="user_id", how="left") \
        .withColumn("habito_final", F.coalesce(col("habito_individual"), col("habito_seller"), lit(0)))

    # --- Guardar resultados ---
    output_transform_path = os.path.join(DATA_CLEAN_PATH, 'final')
    os.makedirs(output_transform_path, exist_ok=True)

    onboarding_final.write.mode("overwrite").parquet(os.path.join(output_transform_path, 'onboarding_final.parquet'))

    print("\n✅ Transformación terminada. Dataset final guardado en /data/processed/clean/final/")

if __name__ == "__main__":
    main()
