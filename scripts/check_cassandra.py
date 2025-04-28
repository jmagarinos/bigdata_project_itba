# scripts/check_cassandra.py

import os
from config.spark_session import get_spark_session

# Definir paths
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

def main():
    # Crear sesión de Spark
    spark = get_spark_session("CheckCassandraOnboarding")

    # Leer datos directamente de Cassandra
    df = spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="users_metrics", keyspace="fintech_onboarding") \
        .load()

    # Mostrar algunos registros
    df.show(10, truncate=False)

    # Mostrar cantidad total de registros
    total = df.count()
    print(f"\n✅ Total de registros en Cassandra: {total}")

if __name__ == "__main__":
    main()
