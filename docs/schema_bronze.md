# Documentação do Schema — Camada Bronze
## Projeto: Lakehouse de Dados Públicos de CNPJ (Receita Federal)

A camada Bronze armazena os dados brutos (raw) obtidos da Receita Federal, convertidos de CSV para Parquet e particionados por data de referência (reference date) e UF. O objetivo é preservar a estrutura original com o mínimo de transformação, permitindo auditoria e rastreabilidade completa.

---

## Estrutura de armazenamento

Todos os dados são particionados por **`data_referencia`** (ano-mês dos arquivos originais) e **`uf`** (unidade federativa) para otimizar consultas e controlar volume:

```
s3://cnpj-lakehouse-xxx/bronze/
├── empresas/
│   ├── data_referencia=2024-12/uf=SP/
│   │   └── part-00000.parquet
│   ├── data_referencia=2024-12/uf=RJ/
│   │   └── part-00000.parquet
│   └── ...
├── estabelecimentos/
│   ├── data_referencia=2024-12/uf=SP/
│   │   └── part-00000.parquet
│   └── ...
├── socios/
├── simples/
└── regime_tributario/
```

---

## 1. `bronze.empresas` (rf_company_root no repo base)

**Origem:** Arquivos `EMPRECSV.zip` da Receita Federal.
**Grão:** Uma empresa por CNPJ raiz (primeiros 8 dígitos).
**Volume esperado:** ~3,5 GB comprimido → ~47 milhões de linhas.

| Coluna | Tipo Postgres (ref) | Tipo Parquet/Big Data | Descrição |
|---|---|---|---|
| `cnpj_basico` | VARCHAR(8) | STRING | Chave primária — 8 primeiros dígitos do CNPJ |
| `razao_social` | VARCHAR(255) | STRING | Nome empresarial |
| `natureza_juridica` | VARCHAR(4) | STRING | Código da natureza jurídica (ex: 2062) |
| `qualificacao_responsavel` | VARCHAR(2) | STRING | Qualificação do responsável legal |
| `capital_social` | NUMERIC(15,2) | DECIMAL(15,2) | Capital social declarado |
| `porte` | VARCHAR(2) | STRING | Porte da empresa (01=ME, 03=EPP, 05=Demais, 00=Não informado) |
| `ente_federativo_responsavel` | VARCHAR(4) | STRING | Preenchido apenas p/ órgãos públicos |
| `data_referencia` | DATE | DATE | Data de referência do arquivo (partition) |
| `uf` | VARCHAR(2) | STRING | Unidade federativa (partition) |

**Origem de dados:** Layout oficial da Receita, campos: 1-8 (CNPJ raiz), 9-158 (razão social), 159-162 (natureza jurídica), etc.

---

## 2. `bronze.estabelecimentos` (rf_company no repo base)

**Origem:** Arquivos `ESTABELE.zip` da Receita Federal.
**Grão:** Um estabelecimento por CNPJ completo (matriz ou filial).
**Volume esperado:** ~1,1 GB comprimido por arquivo × 10 arquivos → ~51 milhões de linhas.

| Coluna | Tipo Postgres (ref) | Tipo Parquet/Big Data | Descrição |
|---|---|---|---|
| `cnpj_basico` | VARCHAR(8) | STRING | Vincula à empresa raiz |
| `cnpj_ordem` | VARCHAR(4) | STRING | Número de ordem do estabelecimento (0001 = matriz) |
| `cnpj_dv` | VARCHAR(2) | STRING | Dígitos verificadores |
| `identificador_matriz_filial` | VARCHAR(1) | STRING | 1 = Matriz, 2 = Filial |
| `nome_fantasia` | VARCHAR(255) | STRING | |
| `situacao_cadastral` | VARCHAR(2) | STRING | 01=Nula, 02=Ativa, 03=Suspensa, 04=Inapta, 08=Baixada |
| `data_situacao_cadastral` | DATE | DATE | |
| `motivo_situacao_cadastral` | VARCHAR(3) | STRING | Código do motivo (se inativa) |
| `nome_cidade_exterior` | VARCHAR(60) | STRING | Preenchido apenas se estabelecimento é no exterior |
| `pais` | VARCHAR(3) | STRING | Código do país (ver `dim_pais`) |
| `data_inicio_atividade` | DATE | DATE | |
| `cnae_fiscal_principal` | VARCHAR(7) | STRING | Código CNAE principal (ex: 4711302) |
| `cnae_fiscal_secundaria` | VARCHAR(2000) | STRING | CNAEs secundários separados por vírgula (pode ser longo) |
| `tipo_logradouro` | VARCHAR(4) | STRING | Ex: Rua, Avenida |
| `logradouro` | VARCHAR(60) | STRING | |
| `numero` | VARCHAR(6) | STRING | |
| `complemento` | VARCHAR(156) | STRING | |
| `bairro` | VARCHAR(50) | STRING | |
| `cep` | VARCHAR(8) | STRING | |
| `uf` | VARCHAR(2) | STRING | (partition) |
| `municipio` | VARCHAR(4) | STRING | Código IBGE |
| `ddd_1` | VARCHAR(2) | STRING | |
| `telefone_1` | VARCHAR(8) | STRING | |
| `ddd_2` | VARCHAR(2) | STRING | |
| `telefone_2` | VARCHAR(8) | STRING | |
| `ddd_fax` | VARCHAR(2) | STRING | |
| `fax` | VARCHAR(8) | STRING | |
| `correio_eletronico` | VARCHAR(255) | STRING | Email |
| `situacao_especial` | VARCHAR(30) | STRING | |
| `data_situacao_especial` | DATE | DATE | |
| `data_referencia` | DATE | DATE | (partition) |

**Nota:** este layout tem 5 colunas a mais que a versão anterior deste documento (`nome_cidade_exterior`, `pais`, `ddd_fax`, `situacao_especial`, `data_situacao_especial`) e não inclui `cnpj_completo` — a Receita não publica essa concatenação, e o pipeline atual (`ingestion/extract_parquet.py`) não calcula nenhuma coluna derivada na camada Bronze/staging (mínima transformação); se for útil, calcular na Silver.

---

## 3. `bronze.socios` (rf_partners no repo base)

**Origem:** Arquivos `SOCIOCSV.zip` da Receita Federal.
**Grão:** Uma relação empresa-sócio (pode haver múltiplos sócios por empresa).
**Volume esperado:** ~250 MB comprimido × 10 arquivos → ~22 milhões de linhas.

| Coluna | Tipo Postgres (ref) | Tipo Parquet/Big Data | Descrição |
|---|---|---|---|
| `cnpj_basico` | VARCHAR(8) | STRING | Vincula à empresa |
| `identificador_de_socio` | VARCHAR(1) | STRING | 1=PJ, 2=PF, 3=Estrangeiro |
| `nome_socio` | VARCHAR(150) | STRING | Nome completo (ou razão social se PJ) |
| `cnpj_cpf_do_socio` | VARCHAR(14) | STRING | CNPJ/CPF sem máscara |
| `qualificacao_do_socio` | VARCHAR(2) | STRING | Código de qualificação |
| `data_entrada_sociedade` | DATE | DATE | |
| `pais` | VARCHAR(3) | STRING | Preenchido se sócio é estrangeiro (ver `dim_pais`) |
| `representante_legal` | VARCHAR(11) | STRING | CPF de quem representa o sócio, se PJ |
| `nome_do_representante` | VARCHAR(150) | STRING | |
| `qualificacao_do_representante_legal` | VARCHAR(2) | STRING | Código de qualificação do representante |
| `faixa_etaria` | VARCHAR(1) | STRING | Faixa codificada (A=até 30, B=30-60, C=60+, etc.) — já é anônima na origem |
| `data_referencia` | DATE | DATE | (partition) |
| `uf` | VARCHAR(2) | STRING | (partition — derivado de cnpj_basico + lookup) |

**Nota:** Pessoa física identificada por `nome_socio` e `cnpj_cpf_do_socio` é dado pessoal. Será mascarado/hasheado na camada Silver (LGPD).

---

## 4. `bronze.simples` (rf_company_root_simples no repo base)

**Origem:** Arquivo `SIMPLES.CSV.zip` da Receita Federal.
**Grão:** Informações de Simples Nacional e MEI por empresa.
**Volume esperado:** ~2 GB → ~30 milhões de linhas.

| Coluna | Tipo Postgres (ref) | Tipo Parquet/Big Data | Descrição |
|---|---|---|---|
| `cnpj_basico` | VARCHAR(8) | STRING | Chave primária |
| `opcao_pelo_simples` | VARCHAR(1) | STRING | S/N — optou pelo Simples Nacional |
| `data_opcao_pelo_simples` | DATE | DATE | |
| `data_exclusao_do_simples` | DATE | DATE | Null se ainda está ativa no Simples |
| `opcao_pelo_mei` | VARCHAR(1) | STRING | S/N — optou por MEI |
| `data_opcao_pelo_mei` | DATE | DATE | |
| `data_exclusao_do_mei` | DATE | DATE | Null se ainda está ativa no MEI |
| `data_referencia` | DATE | DATE | (partition) |
| `uf` | VARCHAR(2) | STRING | (partition) |

---

## 5. `bronze.regime_tributario` (rf_company_tax_regime no repo base)

**Status:** não implementado — não há grupo correspondente em `ingestion/extract_parquet.py::GROUP_CONFIGS`. Seção mantida como referência/roadmap; colunas abaixo ainda não foram validadas contra um arquivo real.

**Origem:** Arquivo `regime-tributario.csv` da Receita Federal.
**Grão:** Informações de regime tributário por CNPJ + data.
**Volume esperado:** ~500 MB → ~10 milhões de linhas.

| Coluna | Tipo Postgres (ref) | Tipo Parquet/Big Data | Descrição |
|---|---|---|---|
| `cnpj_basico` | VARCHAR(8) | STRING | |
| `cnpj_ordem` | VARCHAR(4) | STRING | |
| `cnpj_dv` | VARCHAR(2) | STRING | |
| `cnpj_completo` | VARCHAR(14) | STRING | Calculado |
| `regime_tributario_vigente` | VARCHAR(50) | STRING | Ex: "Lucro Real", "Lucro Presumido" |
| `data_inicio_vigencia` | DATE | DATE | |
| `data_fim_vigencia` | DATE | DATE | Null se ainda vigente |
| `data_referencia` | DATE | DATE | (partition) |
| `uf` | VARCHAR(2) | STRING | (partition) |

---

## 6. Tabelas de domínio (dimensões públicas)

Essas tabelas mapeiam códigos para descrições e vêm também como CSVs do site da Receita. No layout real (confirmado contra o repo base), **todas** têm apenas duas colunas de conteúdo — código e descrição — sem partição por `uf` (só por `data_referencia`); não há colunas extras como `secao` ou nomes específicos como `codigo_ibge`/`nome`:

### `bronze.dim_cnae`
| Coluna | Tipo | Descrição |
|---|---|---|
| `codigo` | STRING | Código CNAE (ex: 4711302) |
| `descricao` | STRING | Descrição da atividade |
| `data_referencia` | DATE | (partition) |

### `bronze.dim_municipio`
| Coluna | Tipo | Descrição |
|---|---|---|
| `codigo` | STRING | Código IBGE (ex: 3550308 para São Paulo) |
| `descricao` | STRING | Nome do município |
| `data_referencia` | DATE | (partition) |

### `bronze.dim_natureza_juridica`
| Coluna | Tipo | Descrição |
|---|---|---|
| `codigo` | STRING | (ex: 2062) |
| `descricao` | STRING | Ex: "Sociedade Empresária Limitada" |
| `data_referencia` | DATE | (partition) |

### `bronze.dim_qualificacao_socio`
| Coluna | Tipo | Descrição |
|---|---|---|
| `codigo` | STRING | (ex: 10) |
| `descricao` | STRING | Ex: "Sócio-Administrador" |
| `data_referencia` | DATE | (partition) |

### `bronze.dim_motivo_deativacao`
| Coluna | Tipo | Descrição |
|---|---|---|
| `codigo` | STRING | |
| `descricao` | STRING | Ex: "Encerramento de atividades" |
| `data_referencia` | DATE | (partition) |

### `bronze.dim_pais`
| Coluna | Tipo | Descrição |
|---|---|---|
| `codigo` | STRING | Código do país (referenciado por `estabelecimentos.pais` e `socios.pais`) |
| `descricao` | STRING | Nome do país |
| `data_referencia` | DATE | (partition) |

---

## Particionamento e formato

- **Partitions:** `data_referencia` (AAAA-MM) + `uf` (ex: SP, RJ)
  - Permite prune eficiente: consultar só SP/2024-12 evita ler todo o dataset
  - Facilita updates incrementais (reprocessar apenas novos meses/UFs)

- **Formato:** Parquet
  - Compressão: snappy (padrão, boa razão compressão/velocidade)
  - Row group size: 128 MB (padrão Parquet, bom para Databricks)

- **Tipagem:**
  - Strings (VARCHAR → STRING) preservam conteúdo sem conversão
  - Dates (DATE → DATE) sem necessidade de parsing na Silver
  - Decimals (NUMERIC → DECIMAL) para valores monetários
  - Nulos (NULL) permitidos em todos os campos opcionais

---

## Fluxo de ingestão

A ingestão é dividida em dois estágios independentes (ver `docs/decisions/0001-split-extract-and-publish.md` para o racional completo e os números de benchmark que motivaram a divisão):

1. **Download** (`ingestion/download.py`): ZIP da Receita → `data/raw/<data_referencia>/`, local.
2. **Extract** (`ingestion/extract_parquet.py`, implementado): lê o CSV de dentro do ZIP em streaming (sem extrair pro disco), via PyArrow, encoding Latin-1. **Sem filtro de UF, sem join por `cnpj_basico`** — grava um Parquet por shard, nacional, em `data/staging/<tabela>/data_referencia=<AAAA-MM>/shard=<N>/`. Camada transiente, não é a Bronze final.
3. **Join + Publish** (**não implementado ainda**): DuckDB lê os Parquets de `data/staging/`, aplica o filtro de UF (Estabelecimentos) e o semi-join por `cnpj_basico` (Empresas/Sócios/Simples), e escreve o resultado final particionado por `data_referencia`/`uf` — a Bronze descrita nas seções acima deste documento — direto em S3 (via `httpfs`).

### Camada de staging (pré-Bronze)

O Parquet gravado pelo estágio de extração tem o **mesmo layout de colunas** de cada tabela Bronze correspondente (seções 1-6 acima), mas difere em dois pontos: (a) é **nacional**, sem filtro de UF/`cnpj_basico` — contém todos os estados; (b) é particionado por `shard=<N>` em vez de `uf=<UF>`, já que ainda não houve join/merge entre shards. Não deve ser consultado diretamente por camadas posteriores (Silver/Gold) — é insumo do estágio de Join + Publish, e será removido depois de uma publicação bem-sucedida em S3 (etapa de limpeza ainda não implementada).

---

## Notas importantes

- **Encoding:** Os CSVs da Receita Federal usam **Latin-1** (ISO-8859-1), não UTF-8. Erro de encoding é muito comum nesse pipeline.
- **Delimitadores:** Campos separados por `;` (ponto-e-vírgula).
- **Strings com espaços em branco:** Alguns campos têm espaços para padding (ex: razão social preenchida a espaços). Será normalizado na Silver.
- **Dados faltantes:** Representados como strings vazias ("") em alguns campos, NULL em outros. Será normalizado na Silver.
- **Volume esperado no Bronze (com UF recortada):** Se filtrar por 1 UF (ex: SP), reduz para ~10-15% do total; se usar 2-3 UFs, fica ~20-30%.

---

## Origem dos metadados

- <cite index="1-1">Layout completo dos arquivos CSV em PDF oficial da Receita Federal</cite>: https://www.gov.br/receitafederal/dados/cnpj-metadados.pdf (também no repo base em `docs/NOVOLAYOUTDOSDADOSABERTOSDOCNPJ.pdf`)
- <cite index="1-1">Repositório de referência (libercapital) com modelos e testes</cite>: https://github.com/libercapital/dados_publicos_cnpj_receita_federal (licença MIT)