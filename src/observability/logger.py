"""Observabilidade simples: registra cada execucao de etapa do pipeline."""
import json
import os
from datetime import datetime, timezone
from pathlib import Path

LOG_DIR = Path("data/observability")
LOG_FILE = LOG_DIR / "pipeline_runs.jsonl"


def registrar(etapa: str, ano: int, status: str, linhas: int = 0, inicio=None, erro: str = None):
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    fim = datetime.now(timezone.utc)
    duracao_s = (fim - inicio).total_seconds() if inicio else None

    registro = {
        "etapa": etapa,
        "ano": ano,
        "status": status,
        "linhas": linhas,
        "inicio": inicio.isoformat() if inicio else None,
        "fim": fim.isoformat(),
        "duracao_s": duracao_s,
        "erro": erro,
    }

    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(registro, ensure_ascii=False) + "\n")

    emoji = "PASS" if status == "sucesso" else "FAIL"
    print(f"[{emoji}] {etapa} ano={ano} linhas={linhas} duracao={duracao_s}")

    resumo = os.environ.get("GITHUB_STEP_SUMMARY")
    if resumo:
        with open(resumo, "a", encoding="utf-8") as f:
            marca = "✅" if status == "sucesso" else "❌"
            linha = f"- {marca} **{etapa}** (ano {ano}): {status}, {linhas} linhas"
            if duracao_s is not None:
                linha += f", {duracao_s:.1f}s"
            if erro:
                linha += f" — erro: `{erro}`"
            f.write(linha + "\n")

    return registro
