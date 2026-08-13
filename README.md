# clima-lakehouse

![Pipeline](https://github.com/joaopedrodelcolli/clima-lakehouse/actions/workflows/pipeline.yml/badge.svg)

Lakehouse pessoal de dados climáticos do Brasil, construído com arquitetura **Medallion**
(Bronze → Silver → Gold) sobre dados públicos do INMET — réplica, em stack 100% open-source, do
tipo de trabalho de engenharia de dados que faço profissionalmente com Microsoft Fabric.

Automatizado via GitHub Actions (ingestão + transformação semanal), com testes de qualidade
(dbt), observabilidade própria e uma camada de IA que permite consultar os dados em linguagem
natural via **MCP (Model Context Protocol)**.

## Por que este projeto

Trabalho como trainee de engenharia de dados usando Microsoft Fabric — um ambiente licenciado
pela empresa, que não pode virar portfólio público. Este projeto replica os mesmos conceitos
(Medallion, modelagem dimensional, qualidade de dados, orquestração) com ferramentas gratuitas
que qualquer pessoa pode clonar e rodar, usando um domínio de dados diferente (clima) para não
reaproveitar nenhum artefato ou dado de cliente.

## Arquitetura
INMET (historico + API)
  -> BRONZE  : ingestao raw, sem transformacao (CSV originais, particionado por ano/estacao)
  -> SILVER  : PySpark + pandas (encoding, tipos, deduplicacao) -> Parquet particionado por ano
  -> GOLD    : modelagem dimensional -> fato_leitura_climatica + dim_estacao + dim_data
               + agregacoes (chuva/regiao, temperatura/UF)
       -> dbt             : testes de qualidade (not_null, unique, accepted_range)
       -> observabilidade : log JSONL de cada execucao do pipeline
       -> servidor MCP    : consulta em linguagem natural via LLM
## Fonte de dados

[Portal de Dados Históricos do INMET](https://portal.inmet.gov.br/dadoshistoricos) — arquivos
anuais públicos, sem autenticação, com um CSV por estação meteorológica automática (medições
horárias desde 2000). Formato real do arquivo (confirmado por inspeção, não pela documentação):
separador `;`, encoding Latin-1, decimal com vírgula, 8 linhas de metadado da estação antes do
cabeçalho, valores faltantes como string vazia.

## Stack

| Camada | Ferramenta | Por quê |
|---|---|---|
| Ingestão | Python (`requests`) | Simples, sem dependências pesadas |
| Transformação | PySpark (local) + pandas | Mesma stack usada profissionalmente |
| Armazenamento | Parquet particionado | Substitui o Lakehouse do Fabric sem custo |
| Modelagem | Esquema estrela (fato + dimensões) | Padrão de mercado para consumo analítico |
| Qualidade de dados | dbt + dbt-duckdb | Testes de not_null, unicidade e range plausível |
| Orquestração | GitHub Actions (cron semanal) | Automação real, com log público de execução |
| Observabilidade | Log JSONL próprio (`registrar()`) | Rastreamento de cada etapa do pipeline |
| Camada de IA | MCP (Model Context Protocol) + Claude Desktop | Consulta em linguagem natural sobre a Gold |

## Camada de IA (MCP)

Um servidor MCP (`src/mcp_server/server.py`) expõe três ferramentas de consulta sobre a camada
Gold, usando DuckDB para ler os Parquet diretamente:

- `temperatura_media_por_uf(uf, ano, mes)`
- `chuva_media_por_regiao(regiao, ano, mes)`
- `estacoes_com_mais_chuva(ano, mes, limite)`

Conectado ao Claude Desktop, isso permite perguntar em português e receber respostas baseadas nos
dados reais do lakehouse. Exemplo de execução real:

> **Pergunta:** Qual foi a temperatura média em SP e a chuva média no Nordeste em janeiro de 2023?
>
> **Resposta (via MCP → dados reais da camada Gold):**
> - Temperatura média em SP: **23,05 °C**
> - Chuva média no Nordeste: **0,1525 mm/h**
> - Estações com mais chuva no mês: Nova Ubirata/MT (552,8 mm), Ouro Branco/MG (544,6 mm),
>   Oliveira/MG (536,2 mm)

## Qualidade de dados

Testes dbt na camada Gold (`dbt_project/models/staging/schema.yml`):

- `dim_estacao.estacao_codigo`: `unique` + `not_null`
- `dim_estacao.latitude`/`longitude`: `not_null`
- `fato_leitura_climatica.estacao_codigo`/`datetime`: `not_null`
- `fato_leitura_climatica.temperatura_c`: intervalo aceito entre -20°C e 55°C (severidade `warn`)

## Observabilidade

Cada execução de cada etapa do pipeline (ingestão, Silver, Gold, dbt) grava um registro em
`data/observability/pipeline_runs.jsonl`: etapa, ano, status, linhas processadas, timestamp de
início/fim e erro (se houver). No GitHub Actions, esse log também aparece formatado no resumo do
job (`GITHUB_STEP_SUMMARY`), com ✅/❌ por etapa.

## Automação

O workflow `.github/workflows/pipeline.yml` roda automaticamente toda segunda-feira (e também sob
demanda via `workflow_dispatch`): baixa os dados do ano corrente, roda Silver, roda Gold
(auto-descobrindo todos os anos já processados, para nunca sobrescrever histórico) e valida tudo
com `dbt build`.

## Estrutura do repositório
clima-lakehouse/
  src/
    ingest/          # download do historico INMET
    transform/        # bronze_to_silver.py, silver_to_gold.py
    observability/     # logger.py
    mcp_server/        # servidor MCP para consulta em linguagem natural
  dbt_project/         # testes de qualidade sobre a camada Gold
  .github/workflows/
    pipeline.yml      # ingestao + transformacao + testes, agendado
  requirements.txt
## Como rodar localmente

```bash
git clone https://github.com/joaopedrodelcolli/clima-lakehouse.git
cd clima-lakehouse
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# pipeline completo para um ano
python src/ingest/baixar_historico.py 2023
python src/transform/bronze_to_silver.py 2023
python src/transform/silver_to_gold.py       # sem argumento: processa todos os anos disponíveis

# testes de qualidade
cd dbt_project && dbt build

# servidor MCP (para conectar a um cliente como Claude Desktop)
python src/mcp_server/server.py
```

## Decisões técnicas e o que aprendi

- **Parquet em vez de Delta Lake**: mais simples de operar localmente sem cluster, mantendo o
  mesmo padrão de particionamento e leitura columnar.
- **Processamento em lotes na conversão pandas → Spark**: necessário por limite de memória do
  ambiente local (WSL2, 4GB); lotes de 20-25 estações evitam OOM sem sacrificar throughput.
- **Auto-descoberta de anos na camada Gold**: a primeira versão sobrescrevia (`mode overwrite`)
  todo o histórico sempre que rodava para um único ano — corrigido para descobrir automaticamente
  todos os anos presentes na Silver antes de reconstruir a Gold.
- **User-Agent de navegador na ingestão**: o servidor do INMET rejeita clientes HTTP sem
  cabeçalho de navegador; necessário simular um `User-Agent` real.
- **DuckDB na camada de consulta**: leitura direta de Parquet sem precisar subir um banco,
  ideal para o footprint pequeno do servidor MCP.

## Autor

João Pedro Del Colli da Silva — trainee de engenharia de dados na DataRocks.
[LinkedIn](https://www.linkedin.com/in/joao-pedro-del-colli) · joao.silva@datarocks.com.br
