# Contexto e Documentação Inicial do Projeto
## Lakehouse de Dados Públicos de CNPJ — Receita Federal

Este documento é o norte do projeto: define objetivo, escopo, arquitetura, decisões técnicas e plano de desenvolvimento. Deve ser atualizado conforme o projeto evolui e serve de referência tanto para o desenvolvimento humano quanto para ferramentas de IA (Claude Code) que venham a apoiar a construção.

---

## 1. Objetivo do projeto

Construir um pipeline de engenharia de dados ponta a ponta (ingestão → armazenamento → transformação → modelagem analítica) como peça de portfólio pessoal, com foco em candidaturas para vagas internacionais de engenharia de dados a partir de 2027.

O projeto deve demonstrar, de forma prática:
- Ingestão de dados públicos reais em larga escala
- Arquitetura de dados em camadas (medallion architecture: Bronze/Silver/Gold)
- Uso de ferramentas de mercado: cloud storage (AWS S3), processamento distribuído (Databricks/PySpark), infraestrutura como código (Terraform), controle de versão (Git) e CI/CD (GitHub Actions)
- Modelagem de dados orientada ao propósito de consumo (analítico, API, ML)
- Boas práticas de segurança e privacidade (LGPD) mesmo trabalhando com dado público

---

## 2. Fonte de dados

**Dados Públicos de CNPJ — Receita Federal do Brasil**

Dataset oficial com informações cadastrais de todas as empresas brasileiras (~60 milhões de CNPJs), disponibilizado publicamente e atualizado mensalmente pela Receita Federal. Inclui empresas, estabelecimentos, sócios, dados do Simples/MEI e tabelas de domínio (CNAE, município, natureza jurídica, etc.).

### Repositório de referência

O projeto reaproveita parte da lógica de download e extração dos arquivos da Receita Federal a partir do repositório:

**[libercapital/dados_publicos_cnpj_receita_federal](https://github.com/libercapital/dados_publicos_cnpj_receita_federal)** (licença MIT)

O que é reaproveitado:
- Lógica de download dos arquivos ZIP da Receita Federal (`src/io/download.py`)
- Lógica de extração/unzip (`src/io/unzip.py`)
- Documentação oficial de layout (PDFs de referência do schema)

O que **não** é reaproveitado (arquitetura divergente):
- Toda a camada de carga em banco relacional (Postgres via `COPY`, `db_models/`, `engine/`) — o projeto usa arquitetura lakehouse (S3 + Delta Lake), não banco relacional
- Orquestração via Docker Compose com Postgres

Crédito ao repositório original deve constar no README final do projeto.

### Decisão de escopo (volume de dados)

O dataset completo tem ~20GB e ~160 milhões de linhas, incompatível com os limites gratuitos utilizados no projeto (S3 free-tier: 5GB; Databricks Community Edition: cluster único e limitado). Escopo definido:

- **Tabelas utilizadas:** Empresas + Estabelecimentos (prioritárias); Sócios e Simples/MEI como extensão se o tempo permitir
- **Filtro de volume:** recorte por UF (a definir) para manter volume manejável, preservando riqueza relacional entre as tabelas

---

## 3. Arquitetura

```
Receita Federal (ZIP/CSV)
   ↓ download (adaptado do repo base)
data/staging (local) — Parquet nacional por shard, sem filtro/join (PyArrow)
   ↓ join por UF/cnpj_basico + publish (DuckDB) — não implementado ainda
S3 — Bronze (raw convertido para Parquet, particionado por data de ingestão + UF)
   ↓ Databricks Community Edition (PySpark)
S3/Delta — Silver (limpeza, tipagem, deduplicação — espelha a estrutura de origem)
   ↓ PySpark
S3/Delta — Gold (modelagem orientada a propósito de consumo)
```

### Decisão de arquitetura de ingestão (extract vs. join/publish)

A ingestão foi dividida em dois estágios independentes — extração (PyArrow, lê os ZIPs e grava Parquet nacional em `data/staging/`, sem filtro) e join+publicação (DuckDB, aplica o filtro de UF/semi-join por `cnpj_basico` e publica em S3 como Bronze) — em vez de um único script fazendo tudo, para aproveitar o ponto forte de cada ferramenta (benchmark comparando pandas/PyArrow/DuckDB nesta decisão). Só o estágio de extração está implementado. Detalhes e números: `docs/decisions/0001-split-extract-and-publish.md`.

### Decisão de modelagem (Gold)

Star schema (Kimball) foi avaliado e **descartado** para as tabelas cadastrais, por não haver eventos de negócio aditivos — os dados da Receita Federal são dados mestres (master data), não transacionais. A modelagem adotada segue três categorias, conforme o propósito de consumo:

| Categoria | Padrão | Tabelas |
|---|---|---|
| Cadastral / analítica | Wide tables desnormalizadas (códigos já resolvidos) | `gold.estabelecimentos`, `gold.socios` |
| Agregado analítico | Fato de verdade (grão definido, métricas aditivas) | `gold.resumo_municipio_cnae_mes` |
| Orientada a API | Grão pequeno, resposta rápida, arrays para drill-down | `gold.socios_resumo`, `gold.empresa_perfil` |
| Orientada a ML | Features prontas + label | `gold.ml_features_empresas` |
| Roadmap (documentado, não implementado na v1) | Série temporal | `gold.serie_temporal_aberturas_baixas` |

Documentação completa de colunas e tipos: ver `docs/schema_gold.md` (gitignored por enquanto — schema ainda em evolução, ver `.gitignore`).

---

## 4. Stack técnica

| Camada | Ferramenta | Observação |
|---|---|---|
| Armazenamento | AWS S3 (free-tier) | Bucket único, prefixos bronze/silver/gold |
| Processamento | Databricks Community Edition | PySpark + Delta Lake |
| IaC | Terraform | Provisiona S3 + IAM (aplicado depois de validar manualmente) |
| Versionamento | Git / GitHub | Repositório público |
| CI/CD | GitHub Actions | Lint, testes, `terraform plan` em PR |
| Linguagem | Python | Scripts de ingestão e transformação |

---

## 5. Convenções do projeto

- **Idioma:** nomes de tabelas, colunas e documentação de domínio de dados em **português** (dado do governo brasileiro). Padrões de código, comentários técnicos e documentação de arquitetura/infra em **inglês**, mantidos em arquivos separados, visando portfólio internacional.
- **Segurança:** credenciais nunca versionadas (`.env` no `.gitignore` desde o primeiro commit; `.env.example` como template público).
- **LGPD:** dados pessoais de pessoa física (nome de sócio) mascarados/hasheados antes de chegar à camada Gold, mesmo sendo dado público oficial.
- **Infraestrutura:** recursos AWS criados manualmente primeiro (para aprendizado), depois traduzidos para Terraform. `terraform destroy` ao final de sessões de estudo prolongadas para evitar custo residual.

---

## 6. Plano de desenvolvimento (visão de uma semana, ajustável ao ritmo real)

| Dia | Entregas |
|---|---|
| 1 | Repositório Git + estrutura de pastas, conta AWS, bucket S3, política IAM restrita, budget alert |
| 2 | Adaptação do script de download/unzip do repo base; ingestão para S3 Bronze (Parquet particionado) |
| 3 | Setup do Databricks Community Edition + acesso ao S3 |
| 4 | Transformação Bronze → Silver (limpeza, tipagem, deduplicação) |
| 5 | Transformação Silver → Gold (wide tables, agregados, tabelas de API e ML) |
| 6 | Terraform (S3 + IAM) + GitHub Actions (lint, testes, terraform plan) |
| 7 | Documentação final (README em inglês, diagrama de arquitetura), créditos ao repositório base, cleanup |

---

## 7. Estrutura do repositório

```
cnpj-lakehouse/
├── README.md                          ← documentação principal, em inglês
├── claude.md                          ← contexto do projeto para Claude Code
├── context.md                         ← este documento
├── docs/
│   ├── schema_bronze.md               ← documentação detalhada das tabelas Bronze + staging
│   ├── schema_gold.md                 ← documentação detalhada das tabelas Gold (gitignored, ver .gitignore)
│   ├── decisions/                     ← ADRs (inglês)
│   │   └── 0001-split-extract-and-publish.md
│   ├── architecture.md                ← diagrama + decisões técnicas (inglês)
│   └── progress.md                    ← log de progresso entre sessões
├── ingestion/
│   ├── download.py                    ← adaptado do repo base
│   ├── extract_parquet.py             ← Estágio 1: ZIP → Parquet staging (PyArrow, sem filtro)
│   └── join_publish.py                ← Estágio 2: filtro/join + publica em S3 (DuckDB) — não implementado ainda
├── notebooks/
│   ├── 01_bronze_to_silver.py
│   └── 02_silver_to_gold.py
├── infra/
│   ├── main.tf
│   ├── variables.tf
│   └── outputs.tf
├── tests/
├── .env.example
├── .gitignore
└── requirements.txt
```

---

## 8. Status atual

- [x] Definição de fonte de dados e escopo
- [x] Modelagem da camada Gold documentada
- [x] Bucket S3 criado e validado
- [x] Política IAM configurada
- [x] Script de extração adaptado (ZIP → Parquet staging, sem filtro — `ingestion/extract_parquet.py`)
- [ ] Script de join/publicação em Bronze/S3 (DuckDB, filtro UF + semi-join `cnpj_basico`)
- [ ] Pipeline Bronze → Silver → Gold implementado
- [ ] Terraform aplicado
- [ ] CI/CD configurado
- [ ] Documentação final e publicação