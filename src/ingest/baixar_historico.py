"""Baixa o histórico de dados horários do INMET e salva raw (Bronze), por ano."""
import sys
from pathlib import Path as _Path
sys.path.insert(0, str(_Path(__file__).resolve().parents[2]))
from datetime import datetime, timezone
from src.observability.logger import registrar
import argparse
import zipfile
from pathlib import Path

import requests

BASE_URL = "https://portal.inmet.gov.br/uploads/dadoshistoricos/{ano}.zip"
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
)
RAW_DIR = Path("data/raw")


def baixar_ano(ano: int, destino: Path = RAW_DIR) -> Path:
    """Baixa e descompacta o ZIP de um ano do histórico do INMET."""
    destino.mkdir(parents=True, exist_ok=True)
    zip_path = destino / f"{ano}.zip"
    extract_dir = destino / str(ano)

    if extract_dir.exists() and any(extract_dir.iterdir()):
        print(f"[{ano}] já extraído em {extract_dir}, pulando download.")
        return extract_dir

    url = BASE_URL.format(ano=ano)
    print(f"[{ano}] baixando {url}")
    resp = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=120)
    resp.raise_for_status()
    zip_path.write_bytes(resp.content)
    print(f"[{ano}] baixado ({len(resp.content) / 1_000_000:.1f} MB)")

    with zipfile.ZipFile(zip_path) as zf:
        zf.extractall(extract_dir)
    print(f"[{ano}] extraído em {extract_dir}")

    zip_path.unlink()
    return extract_dir


def main():
    parser = argparse.ArgumentParser(description="Baixa histórico do INMET por ano.")
    parser.add_argument("anos", nargs="+", type=int, help="Ano(s) a baixar, ex: 2022 2023")
    args = parser.parse_args()

    for ano in args.anos:
        inicio = datetime.now(timezone.utc)
        try:
            destino = baixar_ano(ano)
            n_arquivos = len(list(destino.glob("*.CSV")))
            registrar("ingestao_bronze", ano, "sucesso", linhas=n_arquivos, inicio=inicio)
        except Exception as e:
            registrar("ingestao_bronze", ano, "falha", inicio=inicio, erro=str(e))
            raise


if __name__ == "__main__":
    main()
