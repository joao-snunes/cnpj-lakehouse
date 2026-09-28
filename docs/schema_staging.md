# Staging Parquet Schema

Schema of the Parquet files produced by `ingestion/extract_parquet.py` (the extract stage — see `docs/decisions/0001-split-extract-and-publish.md`). This is **not** the Bronze schema: it's national (no UF filter), split by `shard=<N>` instead of `uf=<UF>`, and has had zero transformation applied beyond mapping the official Receita Federal column layout onto the CSV — every column is stored as `STRING`, including dates and numbers (typing happens later, in Silver). For the final Bronze layout (post UF-filter/join), see `docs/schema_bronze.md`.

## Layout on disk

```
data/staging/<tabela>/
├── data_referencia=<AAAA-MM>/shard=<N>/part-00000.parquet   (sharded groups: estabelecimentos, empresas, socios)
└── data_referencia=<AAAA-MM>/part-00000.parquet             (single-file groups: simples, dim_*)
```

`data_referencia` and, where applicable, `shard` are both a partition directory *and* a real column inside the Parquet file (see `ParquetWriter`/`_process_shard` in `ingestion/extract_parquet.py`) — the file is self-describing even without a Hive-partition-aware reader.

Column order below matches `GROUP_CONFIGS` in `ingestion/extract_parquet.py` exactly (source of truth — keep this doc in sync with that dict if it changes).

## Filter keys (for the future Join + Publish stage)

None of this is applied yet in staging — this section documents which columns the **not-yet-implemented** join/publish stage will actually filter/join on, as opposed to the columns below that only exist as domain lookups (CNAE, município, etc.), which never participate in UF scoping. This mirrors the two filter strategies the original single-pass script (`own_uf` / `by_cnpj_basico`, see the removed `transform_bronze.py`, referenced in `docs/decisions/0001-split-extract-and-publish.md`) used, and that the future stage is expected to reimplement in DuckDB:

| Key | Mechanism | Applies to | Query shape |
|---|---|---|---|
| `estabelecimentos.uf` | Direct predicate — Estabelecimentos is the only table with its own `uf` column | `estabelecimentos` | `WHERE uf = '<UF_FILTRO>'` |
| `cnpj_basico` | Semi-join — propagates the UF filter above to tables that have no `uf` column of their own, via the set of `cnpj_basico` kept from the filtered `estabelecimentos` | `empresas`, `socios`, `simples` | `WHERE cnpj_basico IN (SELECT cnpj_basico FROM estabelecimentos WHERE uf = '<UF_FILTRO>')` |

In other words: **`estabelecimentos.uf` is the only place the UF scoping decision is made** — every other table's UF scoping is inherited transitively through `cnpj_basico`, not decided independently. `cnpj_basico` is therefore the single join key that matters for filtering; every other FK shown in the diagram below (`cnae_fiscal_principal`, `municipio`, `natureza_juridica`, `qualificacao_*`, `motivo_situacao_cadastral`, `pais`) is a **lookup key only** — resolving a code to its description in `dim_*` — and has no bearing on which rows get kept for a given UF.

## `staging.estabelecimentos`

Source: `Estabelecimentos<0-9>.zip`, member matching `ESTABELE`. Sharded (10 files).

| # | Column | Notes |
|---|---|---|
| 1 | `cnpj_basico` | FK → `empresas.cnpj_basico` |
| 2 | `cnpj_ordem` | |
| 3 | `cnpj_dv` | |
| 4 | `identificador_matriz_filial` | 1=Matriz, 2=Filial |
| 5 | `nome_fantasia` | |
| 6 | `situacao_cadastral` | |
| 7 | `data_situacao_cadastral` | |
| 8 | `motivo_situacao_cadastral` | FK → `dim_motivo_deativacao.codigo` |
| 9 | `nome_cidade_exterior` | |
| 10 | `pais` | FK → `dim_pais.codigo` |
| 11 | `data_inicio_atividade` | |
| 12 | `cnae_fiscal_principal` | FK → `dim_cnae.codigo` |
| 13 | `cnae_fiscal_secundaria` | comma-separated list of CNAE codes |
| 14 | `tipo_logradouro` | |
| 15 | `logradouro` | |
| 16 | `numero` | |
| 17 | `complemento` | |
| 18 | `bairro` | |
| 19 | `cep` | |
| 20 | `uf` | 2-letter state code, own column — no filter applied at this stage |
| 21 | `municipio` | FK → `dim_municipio.codigo` (IBGE code) |
| 22 | `ddd_1` | |
| 23 | `telefone_1` | |
| 24 | `ddd_2` | |
| 25 | `telefone_2` | |
| 26 | `ddd_fax` | |
| 27 | `fax` | |
| 28 | `correio_eletronico` | |
| 29 | `situacao_especial` | |
| 30 | `data_situacao_especial` | |
| 31 | `data_referencia` | added by the extract stage (partition) |

## `staging.empresas`

Source: `Empresas<0-9>.zip`, member matching `EMPRECSV`. Sharded (10 files).

| # | Column | Notes |
|---|---|---|
| 1 | `cnpj_basico` | primary key |
| 2 | `razao_social` | |
| 3 | `natureza_juridica` | FK → `dim_natureza_juridica.codigo` |
| 4 | `qualificacao_responsavel` | FK → `dim_qualificacao_socio.codigo` |
| 5 | `capital_social` | decimal, comma as separator, stored as string |
| 6 | `porte` | 01=ME, 03=EPP, 05=Demais, 00=Não informado |
| 7 | `ente_federativo_responsavel` | only filled for public entities |
| 8 | `data_referencia` | added by the extract stage (partition) |

No `uf` column — Empresas has no UF of its own; the future join stage derives it from `estabelecimentos.cnpj_basico`.

## `staging.socios`

Source: `Socios<0-9>.zip`, member matching `SOCIOCSV`. Sharded (10 files).

| # | Column | Notes |
|---|---|---|
| 1 | `cnpj_basico` | FK → `empresas.cnpj_basico` |
| 2 | `identificador_de_socio` | 1=PJ, 2=PF, 3=Estrangeiro |
| 3 | `nome_socio` | PII |
| 4 | `cnpj_cpf_do_socio` | PII |
| 5 | `qualificacao_do_socio` | FK → `dim_qualificacao_socio.codigo` |
| 6 | `data_entrada_sociedade` | |
| 7 | `pais` | FK → `dim_pais.codigo`, filled if sócio is foreign |
| 8 | `representante_legal` | CPF of legal representative, if sócio is PJ; PII |
| 9 | `nome_do_representante` | PII |
| 10 | `qualificacao_do_representante_legal` | FK → `dim_qualificacao_socio.codigo` |
| 11 | `faixa_etaria` | already anonymized at the source (age bracket code) |
| 12 | `data_referencia` | added by the extract stage (partition) |

PII columns (`nome_socio`, `cnpj_cpf_do_socio`, `representante_legal`, `nome_do_representante`) are masked/hashed in Silver, not here — staging mirrors the source as-is.

## `staging.simples`

Source: `Simples.zip`, member matching `SIMPLES`. Single file (not sharded).

| # | Column | Notes |
|---|---|---|
| 1 | `cnpj_basico` | FK → `empresas.cnpj_basico` |
| 2 | `opcao_pelo_simples` | S/N |
| 3 | `data_opcao_pelo_simples` | |
| 4 | `data_exclusao_do_simples` | null if still active |
| 5 | `opcao_pelo_mei` | S/N |
| 6 | `data_opcao_pelo_mei` | |
| 7 | `data_exclusao_do_mei` | null if still active |
| 8 | `data_referencia` | added by the extract stage (partition) |

## Domain tables (`dim_*`)

Source: `Cnaes.zip` / `Municipios.zip` / `Naturezas.zip` / `Qualificacoes.zip` / `Motivos.zip` / `Paises.zip`, one row per code. All single-file, all with the same 2-column layout — `codigo`/`descricao` — plus `data_referencia`. No `uf` partition (these are national reference data, same for every state).

| Table | `codigo` means | `descricao` means |
|---|---|---|
| `staging.dim_cnae` | CNAE code (e.g. `4711302`) | activity description |
| `staging.dim_municipio` | IBGE municipality code | municipality name |
| `staging.dim_natureza_juridica` | legal nature code (e.g. `2062`) | legal nature description |
| `staging.dim_qualificacao_socio` | partner-role code (e.g. `10`) | role description |
| `staging.dim_motivo_deativacao` | deactivation-reason code | reason description |
| `staging.dim_pais` | country code | country name |

## Relationships

```mermaid
erDiagram
    ESTABELECIMENTOS }o--|| EMPRESAS : "cnpj_basico [FILTER KEY - UF propagates through here]"
    SOCIOS }o--|| EMPRESAS : "cnpj_basico [FILTER KEY]"
    SIMPLES }o--|| EMPRESAS : "cnpj_basico [FILTER KEY]"
    ESTABELECIMENTOS }o--o| DIM_CNAE : "cnae_fiscal_principal (lookup only)"
    ESTABELECIMENTOS }o--o| DIM_MUNICIPIO : "municipio (lookup only)"
    ESTABELECIMENTOS }o--o| DIM_MOTIVO_DEATIVACAO : "motivo_situacao_cadastral (lookup only)"
    ESTABELECIMENTOS }o--o| DIM_PAIS : "pais (lookup only)"
    EMPRESAS }o--|| DIM_NATUREZA_JURIDICA : "natureza_juridica (lookup only)"
    EMPRESAS }o--o| DIM_QUALIFICACAO_SOCIO : "qualificacao_responsavel (lookup only)"
    SOCIOS }o--o| DIM_QUALIFICACAO_SOCIO : "qualificacao_do_socio (lookup only)"
    SOCIOS }o--o| DIM_PAIS : "pais (lookup only)"

    EMPRESAS {
        string cnpj_basico PK "join key for the by_cnpj_basico semi-join"
        string razao_social
        string natureza_juridica FK
        string qualificacao_responsavel FK
        string capital_social
        string porte
        string ente_federativo_responsavel
        string data_referencia
    }
    ESTABELECIMENTOS {
        string cnpj_basico FK "join key for the by_cnpj_basico semi-join"
        string cnpj_ordem
        string cnpj_dv
        string identificador_matriz_filial
        string uf "THE filter key - own_uf strategy"
        string municipio FK
        string cnae_fiscal_principal FK
        string motivo_situacao_cadastral FK
        string pais FK
        string data_referencia
    }
    SOCIOS {
        string cnpj_basico FK
        string identificador_de_socio
        string nome_socio
        string cnpj_cpf_do_socio
        string qualificacao_do_socio FK
        string pais FK
        string data_referencia
    }
    SIMPLES {
        string cnpj_basico FK
        string opcao_pelo_simples
        string opcao_pelo_mei
        string data_referencia
    }
    DIM_CNAE { string codigo PK, string descricao }
    DIM_MUNICIPIO { string codigo PK, string descricao }
    DIM_NATUREZA_JURIDICA { string codigo PK, string descricao }
    DIM_QUALIFICACAO_SOCIO { string codigo PK, string descricao }
    DIM_MOTIVO_DEATIVACAO { string codigo PK, string descricao }
    DIM_PAIS { string codigo PK, string descricao }
```

**Important caveat**: these are the relationships the *data* implies, not relationships the staging layer enforces — nothing here has been joined or validated yet (that's the point of the extract stage: no filter, no join). `estabelecimentos`/`socios`/`simples` reference `empresas.cnpj_basico`, but since each staging table is written independently, per shard, with no cross-file check, referential integrity is only guaranteed once the future join/publish stage runs.
