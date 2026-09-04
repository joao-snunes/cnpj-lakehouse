# CLAUDE.md

This file provides guidance to Claude Code (and other AI coding agents) when working in this repository.

## Project overview

A personal portfolio data engineering project: an end-to-end lakehouse pipeline (ingestion → storage → transformation → analytics-ready tables) built on top of Brazil's public CNPJ (company registry) dataset, published monthly by Receita Federal (Brazilian IRS).

Target audience: international data engineering job applications (2027 cycle). Code, technical docs, and architecture decisions are in English. Business-domain naming (table/column names, data dictionaries) is in Portuguese, since the source data is Brazilian government data — see `context.md` for the full rationale.

**Always read `context.md` first** — it is the source of truth for scope, architecture decisions, and current status. Update its "Status atual" checklist as work progresses.

## Architecture

Medallion architecture (Bronze → Silver → Gold), documented in:
- `docs/schema_bronze.md` — raw layer schema (mirrors Receita Federal CSV layout) + the staging layer that precedes it
- `docs/schema_gold.md` — analytics-ready layer schema
- `docs/decisions/` — ADRs for architecture decisions (e.g. `0001-split-extract-and-publish.md`)

Key design decision: **no star schema in Gold**. The CNPJ dataset is master/reference data, not transactional events, so Kimball fact/dimension modeling was deliberately rejected in favor of:
- Wide, denormalized tables for cadastral/analytical use (`gold.estabelecimentos`, `gold.socios`)
- One genuine fact table for aggregated metrics with additive counts (`gold.resumo_municipio_cnae_mes`)
- Small-grain tables optimized for point lookups (`gold.socios_resumo`, `gold.empresa_perfil`)
- A feature table with label, ready for ML (`gold.ml_features_empresas`)

Do not reintroduce star schema / surrogate keys / conformed dimensions unless the underlying data model changes (i.e., if the project starts ingesting real transactional/event data).

## Data source and scope

- Source: Receita Federal public CNPJ files (Empresas, Estabelecimentos, Sócios, Simples/MEI, Regime Tributário)
- Base repository (download/unzip logic adapted from, MIT licensed): https://github.com/libercapital/dados_publicos_cnpj_receita_federal
- **Scope constraint**: full dataset is ~20GB / ~160M rows — incompatible with S3 free-tier (5GB) and Databricks Community Edition. The project filters to a single UF (state) and prioritizes Empresas + Estabelecimentos tables. Do not write code that assumes full-dataset volume; always assume the UF-filtered subset.
- Estabelecimentos/Empresas/Sócios source ZIPs are split into 10 arbitrary chunks each — **not** pre-partitioned by UF. UF filtering happens after reading the CSV content, never at download time. Ingestion itself is split into two stages (`docs/decisions/0001-split-extract-and-publish.md`): extract (ZIP → staging Parquet, no filter, `ingestion/extract_parquet.py`) and join/publish (UF filter + `cnpj_basico` semi-join, then S3 — not implemented yet).

## Conventions

- **Never commit credentials.** `.env` is gitignored from the first commit; `.env.example` is the tracked template.
- **PII handling**: partner (sócio) names and CPF/CNPJ are masked/hashed before reaching the Gold layer, even though the source is public government data.
- **Encoding**: source CSVs from Receita Federal are Latin-1 (ISO-8859-1), not UTF-8, and use `;` as delimiter. Always specify encoding explicitly when reading.
- **Infra**: AWS resources are created manually first (for learning), then translated to Terraform afterward — don't skip straight to Terraform for new resources without confirming with the user first.
- **Partitioning**: Bronze/Silver tables are partitioned by `data_referencia` (YYYY-MM) and `uf`.

## Repository structure

```
cnpj-lakehouse/
├── docs/                    ← source of truth, read before making architecture changes
├── ingestion/               ← download, extract (ZIP→staging Parquet), join/publish (staging→Bronze/S3) scripts (Python)
├── notebooks/               ← PySpark notebooks (Bronze→Silver, Silver→Gold), run on Databricks
├── infra/                   ← Terraform (S3 + IAM)
├── tests/
└── .env.example
```

## Working across machines

This project is developed from two machines (work laptop, personal desktop). There is no shared Claude Code memory between them — treat every session as starting fresh on auto-memory, but rely on:
1. `context.md` status checklist
2. `docs/progress.md` — session log, update at the end of each work session before committing
3. Git as the sync mechanism — always `git pull` before starting, commit + push before ending a session

## Commands

(To be filled in as tooling is set up: linting, tests, terraform plan, etc.)