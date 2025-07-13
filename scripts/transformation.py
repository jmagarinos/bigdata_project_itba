# scripts/transformation.py

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, countDistinct, lit,
    to_date, date_add,
    coalesce, when
)
import pyspark.sql.functions as F

# Definir rutas de proyecto
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
DATA_CLEAN_PATH = os.path.join(PROJECT_ROOT, 'data', 'clean')
DATA_FINAL_PATH = os.path.join(PROJECT_ROOT, 'data', 'final')


def main():
    # 1. Inicializar Spark
    spark = SparkSession.builder \
        .appName("TransformationOnboardingFintech") \
        .getOrCreate()

    # 2. Leer parquet limpios
    lk_users = spark.read.parquet(f"{DATA_CLEAN_PATH}/lk_users_clean.parquet")
    bt_tx    = spark.read.parquet(f"{DATA_CLEAN_PATH}/bt_users_transactions_clean.parquet")
    lk_onb   = spark.read.parquet(f"{DATA_CLEAN_PATH}/lk_onboarding_clean.parquet")

    # 3. Asegurar casteo de tipos
    bt_tx = bt_tx \
        .withColumn("segment", col("segment").cast("integer")) \
        .withColumn("type",    col("type").cast("integer"))
    lk_onb = lk_onb.withColumn("first_login_date", to_date(col("first_login_dt")))

    # 4. Filtrar sólo usuarios de Brasil
    users_brazil = lk_users.filter(col("user_id").startswith("MLB"))

    # 5. Join principal con onboarding
    onboarding_full = users_brazil.join(lk_onb, on="user_id", how="left")

    # 6. Flag "drop"
    onboarding_full = onboarding_full.withColumn(
        "drop",
        when(col("return") == 0, lit(1)).otherwise(lit(0))
    )

    # 7. Preparar transacciones dentro de 30 días de onboarding
    tx = bt_tx.alias("tx") \
        .join(
            lk_onb.select("user_id", "first_login_date"),
            on="user_id", how="inner"
        ) \
        .withColumn("tx_date", to_date(col("transaction_dt"))) \
        .filter(
            (col("tx_date") >= col("first_login_date")) &
            (col("tx_date") <= date_add(col("first_login_date"), 30))
        )

    # 8. Cálculo de hábito para individuals (5 días distintos)
    habito_indiv = (
        tx.filter(col("segment") == 1)
          .groupBy("user_id")
          .agg(countDistinct("tx_date").alias("days_active"))
          .withColumn(
              "habito_individual",
              when(col("days_active") >= 5, lit(1)).otherwise(lit(0))
          )
    )

    # 9. Cálculo de hábito para sellers (5 cobros, tipos 8/9)
    habito_seller = (
        tx.filter((col("segment") == 2) & col("type").isin(8, 9))
          .groupBy("user_id")
          .agg(F.count("*").alias("charges_count"))
          .withColumn(
              "habito_seller",
              when(col("charges_count") >= 5, lit(1)).otherwise(lit(0))
          )
    )

    # 10. Unión de flags y cálculo de hábito final
    onboarding_final = (
        onboarding_full
        .join(habito_indiv,  on="user_id", how="left")
        .join(habito_seller, on="user_id", how="left")
        .withColumn(
            "habito_final",
            coalesce(col("habito_individual"), col("habito_seller"), lit(0))
        )
    )

    # 11. Guardar resultados
    os.makedirs(DATA_FINAL_PATH, exist_ok=True)
    onboarding_final.write.mode("overwrite").parquet(f"{DATA_FINAL_PATH}/onboarding_final.parquet")

    print("✅ Transformación finalizada. Datos listos en data/final/onboarding_final.parquet")


if __name__ == "__main__":
    main()
