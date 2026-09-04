# cnpj-lakehouse

An end-to-end lakehouse pipeline (ingestion → storage → transformation → analytics-ready tables) built on top of Brazil's public CNPJ dataset — the company registry published monthly by Receita Federal (Brazilian IRS), covering ~60 million Brazilian companies.

## Goals

This is a personal portfolio. It's meant to demonstrate, in practice:

- Ingestion of a real, large public dataset (~20GB / ~160M rows uncompressed)
- Layered data architecture (medallion: Bronze → Silver → Gold)
- Market-standard tooling: cloud storage (AWS S3), distributed processing (Databricks/PySpark), infrastructure as code (Terraform), version control, CI/CD
- Data modeling driven by consumption purpose (analytical, API, ML)
- Privacy-conscious handling (LGPD) even though the source data is public

## Current status

| Stage | Status |
|---|---|
| Download ZIPs from Receita Federal (WebDAV) | ✅ `ingestion/download.py` |
| Extract CSV → Parquet staging (no filter/join) | ✅ `ingestion/extract_parquet.py` |
| Filter by UF + join by `cnpj_basico`, publish to S3 (Bronze) | ⏳ not implemented |
| Bronze → Silver (cleaning, typing, dedup) | ⏳ not implemented |
| Silver → Gold (wide tables, aggregates, API/ML tables) | ⏳ not implemented |
| Terraform (S3 + IAM) | ⏳ not implemented |
| CI/CD | ⏳ not implemented |

**Scope constraint**: the full dataset (~20GB) doesn't fit the AWS S3 free tier (5GB) or Databricks Community Edition. The publish stage (not yet built) will filter to a single UF (state), keeping full relational integrity within that state's data. See `UF_FILTRO` in setup below.

## Architecture

```
Receita Federal (ZIP/CSV)
   ↓ download (ingestion/download.py)
data/staging/ (local Parquet, national, per-shard, no filter — ingestion/extract_parquet.py)
   ↓ UF filter + cnpj_basico semi-join + publish (DuckDB) — not implemented yet
S3 — Bronze (Parquet, partitioned by data_referencia + uf)
   ↓ Databricks Community Edition (PySpark) — not implemented yet
S3/Delta — Silver (cleaned, typed, deduplicated)
   ↓ PySpark
S3/Delta — Gold (wide tables, aggregates, API/ML tables)
```

## Repository structure

```
cnpj-lakehouse/
├── README.md                              ← this file
├── CLAUDE.md / claude.md                  ← guidance for AI coding agents working in this repo
├── context.md                             ← project goals, scope, architecture decisions, status
├── docs/
│   ├── schema_bronze.md                   ← Bronze + staging layer schema, column by column
│   ├── schema_gold.md                     ← Gold layer schema (gitignored — still evolving, see below)
│   └── decisions/                         ← ADRs
│       └── 0001-split-extract-and-publish.md
├── ingestion/
│   ├── download.py                        ← downloads ZIPs from Receita Federal's WebDAV share
│   └── extract_parquet.py                 ← extracts ZIPs to staging Parquet (no filter/join)
├── data/                                  ← gitignored — local .zip / Parquet output
│   ├── raw/<data_referencia>/             ← downloaded .zip shards
│   └── staging/<tabela>/...               ← extracted Parquet (national, per shard)
├── pyproject.toml / uv.lock                ← dependencies (uv)
└── .env.example                           ← template for local .env (never commit .env itself)
```

> `docs/schema_gold.md` is intentionally gitignored for now — the Gold layer design is still expected to change as the project evolves, so it's kept local/frozen instead of churning in every commit.

## Setup

Requires Python ≥3.14 and [uv](https://docs.astral.sh/uv/).

```bash
uv sync
cp .env.example .env
```

Fill in `.env`:
- `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_DEFAULT_REGION` / `S3_BUCKET` — only needed once the publish-to-S3 stage exists; not required for download/extract.
- `UF_FILTRO` — the state to eventually filter to (e.g. `MG`); not consumed yet by any implemented stage.
- `SHARDS_<GRUPO>` (e.g. `SHARDS_EMPRESAS`, `SHARDS_ESTABELECIMENTOS`, `SHARDS_SOCIOS`) — optional, restricts which of the 10 shards per group get processed. Overridden per-run by `--shards` on the CLI when passed.

## Usage

### 1. Download

Lists the WebDAV share, resolves the latest available reference month (or a specific one), and downloads the requested file groups' `.zip` shards to `data/raw/<data_referencia>/`.

```bash
# Latest month, default groups (empresas, estabelecimentos)
python ingestion/download.py

# Specific month and groups
python ingestion/download.py --month 2026-08 --groups estabelecimentos,empresas,socios
```

Available groups: `empresas`, `estabelecimentos`, `socios`, `simples`, `cnaes`, `municipios`, `naturezas`, `qualificacoes`, `motivos`, `paises`.

### 2. Extract

Reads each downloaded `.zip` shard's CSV in streaming mode (never extracted to disk — decompressed and parsed on the fly with PyArrow) and writes it to `data/staging/`, one Parquet file per shard, **with no UF filter and no join** — that happens in the not-yet-built publish stage.

```bash
# Same defaults/month-resolution as download.py (latest folder under data/raw/)
python ingestion/extract_parquet.py --groups estabelecimentos,empresas

# A single shard, useful for a quick test or a partial re-run
python ingestion/extract_parquet.py --groups empresas --month 2026-08 --shards 5

# Custom output location
python ingestion/extract_parquet.py --groups cnaes,municipios --output-dir data/staging
```

Output layout: `data/staging/<tabela>/data_referencia=<AAAA-MM>/shard=<N>/part-00000.parquet` for sharded groups (empresas, estabelecimentos, socios), or without the `shard=` level for single-file groups (simples, domain tables).

## Data source and credits

Source: [Receita Federal's public CNPJ dataset](https://www.gov.br/receitafederal/dados/cnpj-metadados.pdf), updated monthly.

Download/extraction logic adapted from [libercapital/dados_publicos_cnpj_receita_federal](https://github.com/libercapital/dados_publicos_cnpj_receita_federal) (MIT licensed) and column-layout cross-checked against [caiopizzol/cnpj-data-pipeline](https://github.com/caiopizzol/cnpj-data-pipeline). Neither repository's database-loading/orchestration layer is reused — this project targets S3 + Parquet/Delta instead of Postgres, per its own architecture decisions (see `context.md`).
