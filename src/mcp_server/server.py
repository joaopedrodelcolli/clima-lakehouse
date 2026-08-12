"""Servidor MCP sobre a camada Gold do Lakehouse climatico (INMET)."""
import sys
from pathlib import Path as _Path
sys.path.insert(0, str(_Path(__file__).resolve().parents[2]))

from pathlib import Path

import duckdb
from mcp.server import MCPServer

GOLD_DIR = Path(__file__).resolve().parents[2] / "data" / "gold"


def temperatura_media_por_uf(uf: str, ano: int, mes: int) -> dict:
    """Retorna a temperatura media (C) de um estado (UF) num mes/ano especifico."""
    con = duckdb.connect(":memory:")
    linha = con.execute(
        f"""
        select uf, ano, mes, temperatura_media_c
        from read_parquet('{GOLD_DIR}/temperatura_mensal_por_uf/*.parquet')
        where uf = ? and ano = ? and mes = ?
        """,
        [uf.upper(), ano, mes],
    ).fetchone()
    if not linha:
        return {"erro": f"Sem dados para {uf}/{ano}-{mes:02d}"}
    return {"uf": linha[0], "ano": linha[1], "mes": linha[2], "temperatura_media_c": round(linha[3], 2)}


def chuva_media_por_regiao(regiao: str, ano: int, mes: int) -> dict:
    """Retorna a precipitacao media (mm/hora) de uma regiao (N, NE, CO, SE, S) num mes/ano."""
    con = duckdb.connect(":memory:")
    linha = con.execute(
        f"""
        select regiao, ano, mes, precipitacao_media_mm
        from read_parquet('{GOLD_DIR}/chuva_mensal_por_regiao/*.parquet')
        where regiao = ? and ano = ? and mes = ?
        """,
        [regiao.upper(), ano, mes],
    ).fetchone()
    if not linha:
        return {"erro": f"Sem dados para regiao {regiao}/{ano}-{mes:02d}"}
    return {"regiao": linha[0], "ano": linha[1], "mes": linha[2], "precipitacao_media_mm": round(linha[3], 4)}


def estacoes_com_mais_chuva(ano: int, mes: int, limite: int = 5) -> list:
    """Lista as estacoes com maior volume total de chuva (mm) num mes/ano."""
    con = duckdb.connect(":memory:")
    linhas = con.execute(
        f"""
        select f.estacao_codigo, e.estacao_nome, e.uf, sum(f.precipitacao_mm) as chuva_total_mm
        from read_parquet('{GOLD_DIR}/fato_leitura_climatica/*/*/*.parquet') f
        join read_parquet('{GOLD_DIR}/dim_estacao/*.parquet') e
          on f.estacao_codigo = e.estacao_codigo
        where f.ano = ? and f.mes = ?
        group by 1, 2, 3
        order by chuva_total_mm desc
        limit ?
        """,
        [ano, mes, limite],
    ).fetchall()
    return [
        {"estacao_codigo": r[0], "estacao_nome": r[1], "uf": r[2], "chuva_total_mm": round(r[3], 1)}
        for r in linhas
    ]


mcp = MCPServer("clima-lakehouse")
mcp.tool()(temperatura_media_por_uf)
mcp.tool()(chuva_media_por_regiao)
mcp.tool()(estacoes_com_mais_chuva)


if __name__ == "__main__":
    mcp.run()
