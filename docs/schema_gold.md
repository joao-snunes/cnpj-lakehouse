# Documentação do Schema — Camada Gold
## Projeto: Lakehouse de Dados Públicos de CNPJ (Receita Federal)

Este documento é o norte de referência para o andamento do projeto. Define todas as tabelas da camada Gold, seus grãos, colunas, propósito e prioridade de implementação.

---

## Visão geral das tabelas

| Tabela | Grão | Categoria | Prioridade |
|---|---|---|---|
| `gold.estabelecimentos` | 1 CNPJ completo (matriz/filial) | Cadastral (wide table) | Essencial |
| `gold.socios` | 1 relação empresa-sócio | Cadastral (wide table) | Essencial |
| `gold.resumo_municipio_cnae_mes` | Município × CNAE × Mês | Agregado analítico | Essencial |
| `gold.socios_resumo` | 1 sócio (pessoa) | Orientada a API | Essencial |
| `gold.ml_features_empresas` | 1 empresa (cnpj_basico) | Orientada a ML | Essencial |
| `gold.empresa_perfil` | 1 empresa (cnpj_basico) | Orientada a API | Bônus |
| `gold.serie_temporal_aberturas_baixas` | Município/UF × Mês | Série temporal / forecasting | Roadmap (documentar, não construir) |

---

## 1. `gold.estabelecimentos`

**Grão:** um registro por CNPJ completo (matriz ou filial).
**Propósito:** wide table cadastral principal — todos os códigos já resolvidos para descrição, pronta para consulta analítica sem necessidade de JOIN.

| Coluna | Tipo | Descrição |
|---|---|---|
| `cnpj_completo` | STRING (PK) | 14 dígitos |
| `cnpj_basico` | STRING (FK lógica) | 8 primeiros dígitos, vincula à empresa/sócios |
| `razao_social` | STRING | |
| `nome_fantasia` | STRING | |
| `situacao_cadastral_descricao` | STRING | Ativa, Baixada, Suspensa, Inapta, Nula |
| `data_situacao_cadastral` | DATE | |
| `motivo_situacao_descricao` | STRING | Motivo da situação atual |
| `cnae_principal_codigo` | STRING | |
| `cnae_principal_descricao` | STRING | |
| `municipio_nome` | STRING | |
| `uf` | STRING | |
| `regiao` | STRING | Derivado da UF |
| `natureza_juridica_descricao` | STRING | |
| `porte_empresa` | STRING | ME, EPP, Demais, Não informado |
| `capital_social` | DECIMAL(15,2) | |
| `data_inicio_atividade` | DATE | |
| `idade_empresa_dias` | INT | Calculado: hoje - data_inicio_atividade |
| `flag_matriz` | BOOLEAN | |
| `flag_optante_simples` | BOOLEAN | |
| `flag_optante_mei` | BOOLEAN | |
| `qtd_socios` | INT | Agregado a partir de `gold.socios` |

---

## 2. `gold.socios`

**Grão:** uma relação empresa-sócio.
**Propósito:** quadro societário achatado, com nome mascarado por padrão (LGPD).

| Coluna | Tipo | Descrição |
|---|---|---|
| `cnpj_basico` | STRING (FK lógica) | Empresa vinculada |
| `nome_socio_mascarado` | STRING | Nome tratado (ex.: "JOÃO S***" ou hash) |
| `tipo_socio` | STRING | PF, PJ, Estrangeiro |
| `qualificacao_descricao` | STRING | Ex.: Sócio-Administrador, Diretor |
| `data_entrada_sociedade` | DATE | |
| `faixa_etaria` | STRING | Faixa codificada pela própria RF (já anonimizada na origem) |

---

## 3. `gold.resumo_municipio_cnae_mes`

**Grão:** Município × CNAE × Ano-Mês.
**Propósito:** único fato de verdade da camada Gold — métricas aditivas derivadas por agregação sobre `estabelecimentos`.

| Coluna | Tipo | Descrição |
|---|---|---|
| `municipio_nome` | STRING | |
| `uf` | STRING | |
| `cnae_descricao` | STRING | |
| `ano_mes` | STRING (PK composta) | Formato `AAAA-MM` |
| `qtd_empresas_ativas` | INT | |
| `qtd_empresas_abertas_mes` | INT | |
| `qtd_empresas_baixadas_mes` | INT | |
| `capital_social_medio` | DECIMAL(15,2) | |

---

## 4. `gold.socios_resumo`

**Grão:** um sócio (pessoa física, jurídica ou estrangeira).
**Propósito:** tabela orientada a consulta pontual tipo API (ex.: `GET /socio/{id}`) — resposta rápida sobre um sócio específico, sem reprocessar a base.

| Coluna | Tipo | Descrição |
|---|---|---|
| `cpf_cnpj_socio_hash` | STRING (PK) | Identificador mascarado/hasheado |
| `nome_socio_mascarado` | STRING | |
| `tipo_socio` | STRING | PF, PJ, Estrangeiro |
| `qtd_empresas_participa` | INT | Sinal relevante: concentração de participações |
| `lista_cnpjs_basicos` | ARRAY\<STRING\> | Permite drill-down |
| `qualificacoes_distintas` | ARRAY\<STRING\> | Todas as qualificações já exercidas |
| `data_primeira_entrada` | DATE | |
| `data_ultima_entrada` | DATE | |
| `uf_predominante` | STRING | UF mais frequente entre as empresas que participa |

---

## 5. `gold.ml_features_empresas`

**Grão:** uma empresa (`cnpj_basico`).
**Propósito:** dataset de features prontas para modelagem — problema natural: prever encerramento (churn/survival) de empresas.

| Coluna | Tipo | Descrição |
|---|---|---|
| `cnpj_basico` | STRING (PK) | |
| `idade_empresa_dias` | INT | Feature numérica |
| `capital_social` | DECIMAL(15,2) | |
| `log_capital_social` | DOUBLE | Transformação log (outliers) |
| `porte_empresa_encoded` | INT | Categórica codificada |
| `natureza_juridica_encoded` | INT | Categórica codificada |
| `cnae_secao` | STRING | Agrupamento setorial (baixa cardinalidade) |
| `qtd_socios` | INT | |
| `qtd_filiais` | INT | |
| `flag_optante_simples` | BOOLEAN | |
| `flag_optante_mei` | BOOLEAN | |
| `densidade_empresas_mesmo_cnae_municipio` | INT | Proxy de concorrência local |
| `situacao_cadastral_target` | INT | **Label**: 0 = ativa, 1 = baixada |

---

## 6. `gold.empresa_perfil` *(bônus)*

**Grão:** uma empresa (`cnpj_basico`), consolidando matriz + filiais.
**Propósito:** endpoint tipo "perfil da empresa" — visão consolidada sem granularidade de estabelecimento individual.

| Coluna | Tipo | Descrição |
|---|---|---|
| `cnpj_basico` | STRING (PK) | |
| `razao_social` | STRING | |
| `situacao_matriz` | STRING | |
| `qtd_filiais` | INT | |
| `qtd_socios` | INT | |
| `capital_social` | DECIMAL(15,2) | |
| `municipios_presente` | ARRAY\<STRING\> | Municípios onde possui estabelecimento |
| `cnaes_praticados` | ARRAY\<STRING\> | Todos os CNAEs (principal + secundários) em uso |

---

## 7. `gold.serie_temporal_aberturas_baixas` *(roadmap — não implementar agora)*

**Grão:** Município ou UF × Ano-Mês.
**Propósito:** série temporal limpa para forecasting (Prophet, ARIMA, regressão) de tendência de abertura/encerramento de empresas por região.

| Coluna | Tipo | Descrição |
|---|---|---|
| `municipio_ou_uf` | STRING | |
| `ano_mes` | STRING | |
| `qtd_aberturas` | INT | |
| `qtd_baixas` | INT | |
| `saldo_liquido` | INT | aberturas - baixas |

> Documentado para fins de visão de roadmap. Não faz parte do escopo da primeira entrega.

---

## Notas transversais

- **LGPD:** todo campo de identificação de pessoa física (`nome_socio`, CPF) é mascarado/hasheado antes de chegar à camada Gold, mesmo sendo dado público oficial.
- **Escopo de volume:** a primeira versão do pipeline deve filtrar por UF e/ou usar apenas os arquivos de Empresas + Estabelecimentos, respeitando os limites do S3 free-tier (5GB) e do Databricks Community Edition.
- **Origem:** todas as tabelas Gold derivam da camada Silver, que espelha a estrutura original da Receita Federal (`silver.empresas`, `silver.estabelecimentos`, `silver.socios`, `silver.simples`, tabelas de domínio).
