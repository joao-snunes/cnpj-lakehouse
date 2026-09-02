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
| `qualificacao_do_responsavel` | VARCHAR(2) | STRING | Qualificação do responsável legal |
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
| `cnpj_completo` | VARCHAR(14) | STRING | CNPJ completo (concatenação de basico+ordem+dv, para facilitar) |
| `matriz_filial` | VARCHAR(1) | STRING | 1 = Matriz, 2 = Filial |
| `nome_fantasia` | VARCHAR(255) | STRING | |
| `situacao_cadastral` | VARCHAR(2) | STRING | 01=Nula, 02=Ativa, 03=Suspensa, 04=Inapta, 08=Baixada |
| `data_situacao_cadastral` | DATE | DATE | |
| `motivo_deativacao` | VARCHAR(3) | STRING | Código do motivo (se inativa) |
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
| `fax` | VARCHAR(8) | STRING | |
| `correio_eletronico` | VARCHAR(255) | STRING | Email |
| `data_referencia` | DATE | DATE | (partition) |

**Nota:** A concatenação `cnpj_completo` não é feita na origem; é um cálculo (`cnpj_basico || cnpj_ordem || cnpj_dv`) que você adiciona já na leitura do CSV para facilitar joins.

---

## 3. `bronze.socios` (rf_partners no repo base)

**Origem:** Arquivos `SOCIOCSV.zip` da Receita Federal.
**Grão:** Uma relação empresa-sócio (pode haver múltiplos sócios por empresa).
**Volume esperado:** ~250 MB comprimido × 10 arquivos → ~22 milhões de linhas.

| Coluna | Tipo Postgres (ref) | Tipo Parquet/Big Data | Descrição |
|---|---|---|---|
| `cnpj_basico` | VARCHAR(8) | STRING | Vincula à empresa |
| `identificador_de_socio` | VARCHAR(1) | STRING | 1=PJ, 2=PF, 3=Estrangeiro |
| `nome_do_socio` | VARCHAR(150) | STRING | Nome completo (ou razão social se PJ) |
| `cnpj_ou_cpf_do_socio` | VARCHAR(14) | STRING | CNPJ/CPF sem máscara |
| `qualificacao_do_socio` | VARCHAR(2) | STRING | Código de qualificação |
| `data_de_entrada_da_sociedade` | DATE | DATE | |
| `cpf_do_representante_legal` | VARCHAR(11) | STRING | Se sócio é PJ, quem a representa |
| `nome_do_representante_legal` | VARCHAR(150) | STRING | |
| `faixa_etaria` | VARCHAR(1) | STRING | Faixa codificada (A=até 30, B=30-60, C=60+, etc.) — já é anônima na origem |
| `data_referencia` | DATE | DATE | (partition) |
| `uf` | VARCHAR(2) | STRING | (partition — derivado de cnpj_basico + lookup) |

**Nota:** Pessoa física identificada por `nome_do_socio` e `cpf_do_socio` é dado pessoal. Será mascarado/hasheado na camada Silver (LGPD).

---

## 4. `bronze.simples` (rf_company_root_simples no repo base)

**Origem:** Arquivo `SIMPLES.CSV.zip` da Receita Federal.
**Grão:** Informações de Simples Nacional e MEI por empresa.
**Volume esperado:** ~2 GB → ~30 milhões de linhas.

| Coluna | Tipo Postgres (ref) | Tipo Parquet/Big Data | Descrição |
|---|---|---|---|
| `cnpj_basico` | VARCHAR(8) | STRING | Chave primária |
| `opcao_pelo_simples` | VARCHAR(1) | STRING | S/N — optou pelo Simples Nacional |
| `data_opcao_simples` | DATE | DATE | |
| `data_exclusao_simples` | DATE | DATE | Null se ainda está ativa no Simples |
| `opcao_pelo_mei` | VARCHAR(1) | STRING | S/N — optou por MEI |
| `data_opcao_mei` | DATE | DATE | |
| `data_exclusao_mei` | DATE | DATE | Null se ainda está ativa no MEI |
| `data_referencia` | DATE | DATE | (partition) |
| `uf` | VARCHAR(2) | STRING | (partition) |

---

## 5. `bronze.regime_tributario` (rf_company_tax_regime no repo base)

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

Essas tabelas mapeiam códigos para descrições e vêm também como CSVs do site da Receita:

### `bronze.dim_cnae`
| Coluna | Tipo | Descrição |
|---|---|---|
| `codigo` | STRING | Código CNAE (ex: 4711302) |
| `descricao` | STRING | Descrição da atividade |
| `secao` | STRING | Seção agregada (ex: G, H, I) |
| `data_referencia` | DATE | (partition) |

### `bronze.dim_municipio`
| Coluna | Tipo | Descrição |
|---|---|---|
| `codigo_ibge` | STRING | Código IBGE (ex: 3550308 para São Paulo) |
| `nome` | STRING | Nome do município |
| `uf` | STRING | (partition) |
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

1. **Download:** ZIP da Receita → local
2. **Unzip:** `.zip` → `.csv`
3. **Read:** Pandas lê CSV com encoding correto (Latin-1 pela RFC dos arquivos)
4. **Transform (mínimo):**
   - Tipagem básica (DATE, INT, STRING)
   - Concatenação `cnpj_completo` onde necessário
   - Add `data_referencia` e `uf` (colunas de partition)
5. **Write:** Parquet particionado → S3

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