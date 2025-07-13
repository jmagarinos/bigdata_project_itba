# scripts/transformation.py

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct, lit, to_date, date_add, coalesce, when
import pyspark.sql.functions as F

# Rutas de proyecto
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
DATA_CLEAN_PATH = os.path.join(PROJECT_ROOT, 'data', 'clean')

# Ruta de salida final
DATA_FINAL_PATH = os.path.join(PROJECT_ROOT, 'data', 'final')


def main():
    # Inicializar Spark
    spark = SparkSession.builder \
        .appName("TransformationOnboardingFintech") \
        .getOrCreate()

    # Leer datasets limpios
    lk_users              = spark.read.parquet(f"{DATA_CLEAN_PATH}/lk_users_clean.parquet")
    bt_users_transactions = spark.read.parquet(f"{DATA_CLEAN_PATH}/bt_users_transactions_clean.parquet")
    lk_onboarding         = spark.read.parquet(f"{DATA_CLEAN_PATH}/lk_onboarding_clean.parquet")

    # Asegurar tipos correctos en bt_users_transactions
    bt_users_transactions = bt_users_transactions \
        .withColumn("segment", col("segment").cast("integer")) \
        .withColumn("type",    col("type").cast("integer"))

    # Filtrar usuarios de Brasil
    users_brazil = lk_users.filter(col("user_id").startswith("MLB"))

    # Join principal con onboarding
    onboarding_full = users_brazil.join(lk_onboarding, on="user_id", how="left")

    # Calcular flag "drop": usuarios que no retornaron tras primer login
    onboarding_full = onboarding_full.withColumn(
        "drop",
        when(col("return") == 0, lit(1)).otherwise(lit(0))
    )

    # Activación y setup ya vienen como flags en lk_onboarding_clean

    # Preparar transacciones dentro de ventana de 30 días desde el primer login
    tx = bt_users_transactions.alias("tx") \
        .join(
            lk_onboarding.select("user_id", "first_login_dt"),
            on="user_id", how="inner"
        ) \
        .withColumn("tx_date", to_date(col("transaction_dt"))) \
        .filter(
            (col("tx_date") >= col("first_login_dt")) &
            (col("tx_date") <= date_add(col("first_login_dt"), 30))
        )

    # Hábito para individuals: 5 días distintos con transacciones
    habito_individual = (
        tx.filter(col("segment") == 1)
          .groupBy("user_id")
          .agg(countDistinct("tx_date").alias("days_active"))
          .withColumn(
              "habito_individual",
              when(col("days_active") >= 5, lit(1)).otherwise(lit(0))
          )
    )

    # Hábito para sellers: 5 transacciones de cobro (type 8/9)
    habito_seller = (
        tx.filter((col("segment") == 2) & col("type").isin(8, 9))
          .groupBy("user_id")
          .agg(F.count("*").alias("charges_count"))
          .withColumn(
              "habito_seller",
              when(col("charges_count") >= 5, lit(1)).otherwise(lit(0))
          )
    )

    # Unir flags de hábito y calcular hábito final
    onboarding_final = (
        onboarding_full
        .join(habito_individual, on="user_id", how="left")
        .join(habito_seller,     on="user_id", how="left")
        .withColumn(
            "habito_final",
            coalesce(col("habito_individual"), col("habito_seller"), lit(0))
        )
    )

    # Guardar resultado final
    os.makedirs(DATA_FINAL_PATH, exist_ok=True)
    onboarding_final.write.mode("overwrite").parquet(f"{DATA_FINAL_PATH}/onboarding_final.parquet")

    print("✅ Transformación completada. Datos guardados en data/final/onboarding_final.parquet")


if __name__ == "__main__":
    main()
