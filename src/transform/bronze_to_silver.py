"""Bronze -> Silver: limpeza e padronizacao dos dados horarios do INMET."""
import sys
from pathlib import Path as _Path
sys.path.insert(0, str(_Path(__file__).resolve().parents[2]))
from datetime import datetime, timezone
from src.observability.logger import registrar
import argparse
import glob
from pathlib import Path

import pandas as pd
from pyspark.sql import SparkSession

RAW_DIR = Path("data/raw")
SILVER_DIR = Path("data/silver")
BATCH_SIZE = 25

COLUNAS_RENOMEADAS = {
    "PRECIPITAÇÃO TOTAL, HORÁRIO (mm)": "precipitacao_mm",
    "PRESSAO ATMOSFERICA AO NIVEL DA ESTACAO, HORARIA (mB)": "pressao_mb",
    "PRESSÃO ATMOSFERICA MAX.NA HORA ANT. (AUT) (mB)": "pressao_max_hora_ant_mb",
    "PRESSÃO ATMOSFERICA MIN. NA HORA ANT. (AUT) (mB)": "pressao_min_hora_ant_mb",
    "RADIACAO GLOBAL (Kj/m²)": "radiacao_kj_m2",
    "TEMPERATURA DO AR - BULBO SECO, HORARIA (°C)": "temperatura_c",
    "TEMPERATURA DO PONTO DE ORVALHO (°C)": "temperatura_orvalho_c",
    "TEMPERATURA MÁXIMA NA HORA ANT. (AUT) (°C)": "temperatura_max_hora_ant_c",
    "TEMPERATURA MÍNIMA NA HORA ANT. (AUT) (°C)": "temperatura_min_hora_ant_c",
    "TEMPERATURA ORVALHO MAX. NA HORA ANT. (AUT) (°C)": "temperatura_orvalho_max_hora_ant_c",
    "TEMPERATURA ORVALHO MIN. NA HORA ANT. (AUT) (°C)": "temperatura_orvalho_min_hora_ant_c",
    "UMIDADE REL. MAX. NA HORA ANT. (AUT) (%)": "umidade_max_hora_ant_pct",
    "UMIDADE REL. MIN. NA HORA ANT. (AUT) (%)": "umidade_min_hora_ant_pct",
    "UMIDADE RELATIVA DO AR, HORARIA (%)": "umidade_pct",
    "VENTO, DIREÇÃO HORARIA (gr) (° (gr))": "vento_direcao_gr",
    "VENTO, RAJADA MAXIMA (m/s)": "vento_rajada_maxima_ms",
    "VENTO, VELOCIDADE HORARIA (m/s)": "vento_velocidade_ms",
}


def parse_float_seguro(valor):
    """Converte texto pt-BR pra float, tratando NULL/vazio como ausente (None)."""
    if valor is None:
        return None
    valor = valor.strip()
    if valor == "" or valor.upper() == "NULL":
        return None
    try:
        return float(valor.replace(",", "."))
    except ValueError:
        return None


def ler_metadado_estacao(caminho: Path) -> dict:
    with open(caminho, encoding="latin-1") as f:
        linhas = [next(f).strip() for _ in range(8)]
    campos = {}
    for linha in linhas:
        chave, _, valor = linha.partition(";")
        campos[chave.replace(":", "").strip()] = valor.strip()
    return {
        "regiao": campos.get("REGIAO"),
        "uf": campos.get("UF"),
        "estacao_nome": campos.get("ESTACAO"),
        "estacao_codigo": campos.get("CODIGO (WMO)"),
        "latitude": parse_float_seguro(campos.get("LATITUDE")),
        "longitude": parse_float_seguro(campos.get("LONGITUDE")),
        "altitude": parse_float_seguro(campos.get("ALTITUDE")),
    }


def ler_estacao(caminho: Path) -> pd.DataFrame:
    meta = ler_metadado_estacao(caminho)
    df = pd.read_csv(caminho, sep=";", encoding="latin-1", skiprows=8, decimal=",")
    df = df.drop(columns=[c for c in df.columns if c.startswith("Unnamed")])
    df["datetime"] = pd.to_datetime(
        df["Data"] + " " + df["Hora UTC"].str.replace(" UTC", ""),
        format="%Y/%m/%d %H%M",
    )
    df = df.drop(columns=["Data", "Hora UTC"])
    df = df.rename(columns=COLUNAS_RENOMEADAS)
    for chave, valor in meta.items():
        df[chave] = valor
    return df


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("anos", nargs="+", type=int)
    args = parser.parse_args()

    spark = (
        SparkSession.builder.appName("inmet-silver")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )

    for ano in args.anos:
        inicio_execucao = datetime.now(timezone.utc)
        try:
            arquivos = sorted(glob.glob(str(RAW_DIR / str(ano) / "*.CSV")))
            print(f"[{ano}] {len(arquivos)} estacoes encontradas, em lotes de {BATCH_SIZE}")

            destino = SILVER_DIR / f"ano={ano}"
            total_linhas = 0

            for inicio_lote in range(0, len(arquivos), BATCH_SIZE):
                lote = arquivos[inicio_lote: inicio_lote + BATCH_SIZE]
                partes = [ler_estacao(Path(a)) for a in lote]
                df_lote = pd.concat(partes, ignore_index=True)

                df_spark = spark.createDataFrame(df_lote)
                df_spark = df_spark.dropDuplicates(["estacao_codigo", "datetime"])

                modo = "overwrite" if inicio_lote == 0 else "append"
                df_spark.write.mode(modo).parquet(str(destino))

                n = df_spark.count()
                total_linhas += n
                print(f"[{ano}] lote {inicio_lote // BATCH_SIZE + 1} gravado ({len(lote)} estacoes, {n} linhas)")

            print(f"[{ano}] concluido: {total_linhas} linhas gravadas em {destino}")
            registrar("transformacao_silver", ano, "sucesso", linhas=total_linhas, inicio=inicio_execucao)
        except Exception as e:
            registrar("transformacao_silver", ano, "falha", inicio=inicio_execucao, erro=str(e))
            raise

    spark.stop()


if __name__ == "__main__":
    main()
