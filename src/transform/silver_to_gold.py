"""Silver -> Gold: modelagem dimensional (fato + dimensoes) e agregacoes."""
import argparse
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

SILVER_DIR = Path("data/silver")
GOLD_DIR = Path("data/gold")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("anos", nargs="+", type=int)
    args = parser.parse_args()

    spark = (
        SparkSession.builder.appName("inmet-gold")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )

    caminhos = [str(SILVER_DIR / f"ano={ano}") for ano in args.anos]
    df = spark.read.parquet(*caminhos)

    dim_estacao = df.select(
        "estacao_codigo", "estacao_nome", "uf", "regiao",
        "latitude", "longitude", "altitude",
    ).dropDuplicates(["estacao_codigo"])

    dim_data = (
        df.select(F.to_date("datetime").alias("data"))
        .dropDuplicates(["data"])
        .withColumn("ano", F.year("data"))
        .withColumn("mes", F.month("data"))
        .withColumn("dia", F.dayofmonth("data"))
        .withColumn("trimestre", F.quarter("data"))
        .withColumn(
            "estacao_do_ano",
            F.when(F.col("mes").isin(12, 1, 2), "verao")
            .when(F.col("mes").isin(3, 4, 5), "outono")
            .when(F.col("mes").isin(6, 7, 8), "inverno")
            .otherwise("primavera"),
        )
    )

    fato = df.select(
        F.to_date("datetime").alias("data"),
        F.year("datetime").alias("ano"),
        F.month("datetime").alias("mes"),
        "datetime",
        "estacao_codigo",
        "temperatura_c",
        "temperatura_orvalho_c",
        "precipitacao_mm",
        "pressao_mb",
        "umidade_pct",
        "vento_velocidade_ms",
        "vento_direcao_gr",
        "radiacao_kj_m2",
    )

    chuva_mensal_por_regiao = (
        df.withColumn("ano", F.year("datetime"))
        .withColumn("mes", F.month("datetime"))
        .groupBy("regiao", "ano", "mes")
        .agg(F.avg("precipitacao_mm").alias("precipitacao_media_mm"))
        .orderBy("regiao", "ano", "mes")
    )

    temperatura_mensal_por_uf = (
        df.withColumn("ano", F.year("datetime"))
        .withColumn("mes", F.month("datetime"))
        .groupBy("uf", "ano", "mes")
        .agg(F.avg("temperatura_c").alias("temperatura_media_c"))
        .orderBy("uf", "ano", "mes")
    )

    dim_estacao.write.mode("overwrite").parquet(str(GOLD_DIR / "dim_estacao"))
    dim_data.write.mode("overwrite").parquet(str(GOLD_DIR / "dim_data"))
    fato.write.mode("overwrite").partitionBy("ano", "mes").parquet(str(GOLD_DIR / "fato_leitura_climatica"))
    chuva_mensal_por_regiao.write.mode("overwrite").parquet(str(GOLD_DIR / "chuva_mensal_por_regiao"))
    temperatura_mensal_por_uf.write.mode("overwrite").parquet(str(GOLD_DIR / "temperatura_mensal_por_uf"))

    print("dim_estacao:", dim_estacao.count())
    print("dim_data:", dim_data.count())
    print("fato_leitura_climatica:", fato.count())

    spark.stop()


if __name__ == "__main__":
    main()
