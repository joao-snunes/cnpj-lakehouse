"""
Extrai ZIPs da Receita Federal já baixados por download.py, filtra por UF e
grava Parquet particionado para a camada Bronze.

Aceita um grupo ou lista de grupos (mesmo espírito do FILE_GROUPS de
download.py), então adicionar suporte a um novo grupo no futuro é: (1) achar
o member_pattern/colunas oficiais do arquivo, (2) adicionar uma entrada em
GROUP_CONFIGS. Nenhuma outra função precisa mudar.

Lê de data/raw/<data_referencia>/ e grava em:

    data/bronze/<tabela>/data_referencia=<AAAA-MM>/uf=<UF>/part-00000.parquet   (grupos com filtro de UF)
    data/bronze/<tabela>/data_referencia=<AAAA-MM>/part-00000.parquet          (tabelas de domínio, sem UF)

Colunas confirmadas contra github.com/caiopizzol/cnpj-data-pipeline
(processor.py), que espelha o layout oficial dos CSVs da Receita Federal.
docs/schema_bronze.md diverge do layout real em alguns pontos (ex:
Estabelecimentos tem 5 colunas a mais — nome_cidade_exterior, pais, ddd_fax,
situacao_especial, data_situacao_especial — e Sócios usa nomes/quantidade de
colunas diferentes); este script usa o layout real e a doc deve ser
atualizada para bater.

Estratégias de filtro por grupo:
  - "own_uf": o arquivo tem coluna uf própria (só Estabelecimentos). Filtra
    direto e acumula o cnpj_basico de cada linha mantida.
  - "by_cnpj_basico": o arquivo não tem uf própria (Empresas, Sócios,
    Simples/MEI), mas tem cnpj_basico. É filtrado contra o conjunto de
    cnpj_basico produzido por "own_uf" — nesta execução (se
    'estabelecimentos' também foi pedido) ou lido de volta do Parquet já
    gravado anteriormente para essa data_referencia/UF.
  - "none": tabelas de domínio/referência (CNAE, município, natureza
    jurídica, etc.) — sem filtro, copiadas por completo.
"""

import argparse
import io
import logging
import os
import sys
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

RAW_DIR = Path("data/raw")
BRONZE_DIR = Path("data/bronze")
CHUNK_SIZE = 200_000

# Sigla de 2 letras (ex: "MG"), igual ao valor literal da coluna "uf" do
# ESTABELE — não é o código IBGE numérico da UF.
UF_FILTRO = os.getenv("UF_FILTRO", "MG").upper()

FilterStrategy = Literal["own_uf", "by_cnpj_basico", "none"]


@dataclass(frozen=True)
class GroupConfig:
    zip_prefix: str  # nome do zip é f"{zip_prefix}{shard}.zip" (shard='' se não fragmentado)
    member_pattern: str  # substring (maiúscula) do nome do CSV dentro do zip
    columns: list[str]  # layout oficial, na ordem, para o CSV sem header
    table_name: str  # nome da tabela Bronze de saída
    sharded: bool  # True: 10 arquivos numerados 0..9; False: arquivo único
    filter_strategy: FilterStrategy
    partition_by_uf: bool  # se o caminho de saída inclui uf=<UF>


# Colunas oficiais confirmadas contra o cnpj-data-pipeline (processor.py).
_EMPRESAS_COLUMNS = [
    "cnpj_basico",
    "razao_social",
    "natureza_juridica",
    "qualificacao_responsavel",
    "capital_social",
    "porte",
    "ente_federativo_responsavel",
]

_ESTABELECIMENTOS_COLUMNS = [
    "cnpj_basico",
    "cnpj_ordem",
    "cnpj_dv",
    "identificador_matriz_filial",
    "nome_fantasia",
    "situacao_cadastral",
    "data_situacao_cadastral",
    "motivo_situacao_cadastral",
    "nome_cidade_exterior",
    "pais",
    "data_inicio_atividade",
    "cnae_fiscal_principal",
    "cnae_fiscal_secundaria",
    "tipo_logradouro",
    "logradouro",
    "numero",
    "complemento",
    "bairro",
    "cep",
    "uf",
    "municipio",
    "ddd_1",
    "telefone_1",
    "ddd_2",
    "telefone_2",
    "ddd_fax",
    "fax",
    "correio_eletronico",
    "situacao_especial",
    "data_situacao_especial",
]

_SOCIOS_COLUMNS = [
    "cnpj_basico",
    "identificador_de_socio",
    "nome_socio",
    "cnpj_cpf_do_socio",
    "qualificacao_do_socio",
    "data_entrada_sociedade",
    "pais",
    "representante_legal",
    "nome_do_representante",
    "qualificacao_do_representante_legal",
    "faixa_etaria",
]

_SIMPLES_COLUMNS = [
    "cnpj_basico",
    "opcao_pelo_simples",
    "data_opcao_pelo_simples",
    "data_exclusao_do_simples",
    "opcao_pelo_mei",
    "data_opcao_pelo_mei",
    "data_exclusao_do_mei",
]

_DOMINIO_COLUMNS = ["codigo", "descricao"]

# Um grupo por entrada de FILE_GROUPS em download.py. Adicionar um grupo novo
# = uma entrada aqui, sem tocar no resto do arquivo.
GROUP_CONFIGS: dict[str, GroupConfig] = {
    "estabelecimentos": GroupConfig(
        "Estabelecimentos", "ESTABELE", _ESTABELECIMENTOS_COLUMNS, "estabelecimentos", True, "own_uf", True
    ),
    "empresas": GroupConfig("Empresas", "EMPRECSV", _EMPRESAS_COLUMNS, "empresas", True, "by_cnpj_basico", True),
    "socios": GroupConfig("Socios", "SOCIOCSV", _SOCIOS_COLUMNS, "socios", True, "by_cnpj_basico", True),
    "simples": GroupConfig("Simples", "SIMPLES", _SIMPLES_COLUMNS, "simples", False, "by_cnpj_basico", True),
    "cnaes": GroupConfig("Cnaes", "CNAECSV", _DOMINIO_COLUMNS, "dim_cnae", False, "none", False),
    "municipios": GroupConfig("Municipios", "MUNICCSV", _DOMINIO_COLUMNS, "dim_municipio", False, "none", False),
    "naturezas": GroupConfig(
        "Naturezas", "NATJUCSV", _DOMINIO_COLUMNS, "dim_natureza_juridica", False, "none", False
    ),
    "qualificacoes": GroupConfig(
        "Qualificacoes", "QUALSCSV", _DOMINIO_COLUMNS, "dim_qualificacao_socio", False, "none", False
    ),
    "motivos": GroupConfig("Motivos", "MOTICSV", _DOMINIO_COLUMNS, "dim_motivo_deativacao", False, "none", False),
    "paises": GroupConfig("Paises", "PAISCSV", _DOMINIO_COLUMNS, "dim_pais", False, "none", False),
}


def _get_shards(group: str, cfg: GroupConfig) -> list[str]:
    """Shards a processar para um grupo. Grupos não fragmentados sempre retornam [''],
    resultando no nome de arquivo f"{zip_prefix}.zip"."""
    if not cfg.sharded:
        return [""]
    env_var = f"SHARDS_{group.upper()}"
    default = ",".join(str(i) for i in range(10))
    return [s.strip() for s in os.getenv(env_var, default).split(",") if s.strip()]


def _find_zip_member(zip_ref: zipfile.ZipFile, pattern: str) -> str:
    for name in zip_ref.namelist():
        if pattern in name.upper():
            return name
    raise FileNotFoundError(f"Nenhum membro contendo '{pattern}' em {zip_ref.filename}")


def _read_csv_chunks(zip_path: Path, member_pattern: str, columns: list[str]):
    """Itera em chunks um CSV sem header, separado por ';', Latin-1, direto de dentro do ZIP
    (sem extrair para disco — zip_ref.open() descomprime sob demanda enquanto o pandas lê)."""
    with zipfile.ZipFile(zip_path) as zip_ref:
        member = _find_zip_member(zip_ref, member_pattern)
        with zip_ref.open(member) as raw:
            text_stream = io.TextIOWrapper(raw, encoding="latin-1", newline="")
            yield from pd.read_csv(
                text_stream,
                sep=";",
                header=None,
                names=columns,
                dtype=str,
                na_values=[""],
                keep_default_na=False,
                chunksize=CHUNK_SIZE,
                engine="c",
            )


class PartitionWriter:
    """Escreve chunks de DataFrame em um único arquivo Parquet por tabela,
    particionado por data_referencia (+ uf, quando aplicável) via diretórios."""

    def __init__(self, table_name: str, reference_date: str, uf: str | None):
        out_dir = BRONZE_DIR / table_name / f"data_referencia={reference_date}"
        if uf is not None:
            out_dir = out_dir / f"uf={uf}"
        out_dir.mkdir(parents=True, exist_ok=True)
        self.path = out_dir / "part-00000.parquet"
        self._writer: pq.ParquetWriter | None = None
        self.rows = 0

    def write(self, df: pd.DataFrame) -> None:
        if df.empty:
            return
        table = pa.Table.from_pandas(df, preserve_index=False)
        if self._writer is None:
            self._writer = pq.ParquetWriter(str(self.path), table.schema, compression="snappy")
        self._writer.write_table(table)
        self.rows += len(df)

    def close(self) -> None:
        if self._writer is not None:
            self._writer.close()


def _iter_shards(cfg: GroupConfig, raw_dir: Path, shards: list[str]):
    for shard in shards:
        zip_path = raw_dir / f"{cfg.zip_prefix}{shard}.zip"
        if not zip_path.exists():
            logger.warning("Shard não encontrado, pulando: %s", zip_path)
            continue
        logger.info("Lendo %s", zip_path.name)
        yield zip_path


def _process_own_uf(cfg: GroupConfig, raw_dir: Path, reference_date: str, shards: list[str]) -> set[str]:
    """Filtra pela própria coluna uf; retorna o conjunto de cnpj_basico mantidos
    (usado pelos grupos 'by_cnpj_basico')."""
    matched: set[str] = set()
    writer = PartitionWriter(cfg.table_name, reference_date, UF_FILTRO if cfg.partition_by_uf else None)

    for zip_path in _iter_shards(cfg, raw_dir, shards):
        for chunk in _read_csv_chunks(zip_path, cfg.member_pattern, cfg.columns):
            filtered = chunk[chunk["uf"] == UF_FILTRO].copy()
            if filtered.empty:
                continue
            matched.update(filtered["cnpj_basico"].tolist())
            filtered["data_referencia"] = reference_date
            writer.write(filtered)

    writer.close()
    logger.info("%s: %d linhas gravadas em %s", cfg.table_name, writer.rows, writer.path)
    return matched


def _process_by_cnpj_basico(
    cfg: GroupConfig, raw_dir: Path, reference_date: str, shards: list[str], matched_cnpj_basico: set[str]
) -> None:
    writer = PartitionWriter(cfg.table_name, reference_date, UF_FILTRO if cfg.partition_by_uf else None)

    for zip_path in _iter_shards(cfg, raw_dir, shards):
        for chunk in _read_csv_chunks(zip_path, cfg.member_pattern, cfg.columns):
            filtered = chunk[chunk["cnpj_basico"].isin(matched_cnpj_basico)].copy()
            if filtered.empty:
                continue
            if cfg.partition_by_uf:
                filtered["uf"] = UF_FILTRO
            filtered["data_referencia"] = reference_date
            writer.write(filtered)

    writer.close()
    logger.info("%s: %d linhas gravadas em %s", cfg.table_name, writer.rows, writer.path)


def _process_none(cfg: GroupConfig, raw_dir: Path, reference_date: str, shards: list[str]) -> None:
    """Tabelas de domínio/referência: sem filtro de UF, copiadas por completo."""
    writer = PartitionWriter(cfg.table_name, reference_date, None)

    for zip_path in _iter_shards(cfg, raw_dir, shards):
        for chunk in _read_csv_chunks(zip_path, cfg.member_pattern, cfg.columns):
            chunk = chunk.copy()
            chunk["data_referencia"] = reference_date
            writer.write(chunk)

    writer.close()
    logger.info("%s: %d linhas gravadas em %s", cfg.table_name, writer.rows, writer.path)


def _load_matched_cnpj_basico(reference_date: str) -> set[str]:
    """Lê de volta o cnpj_basico já gravado em bronze.estabelecimentos para esta
    data_referencia/UF, para grupos 'by_cnpj_basico' rodados sem 'estabelecimentos'
    na mesma execução (ex: reprocessar só 'socios' depois)."""
    path = (
        BRONZE_DIR / "estabelecimentos" / f"data_referencia={reference_date}" / f"uf={UF_FILTRO}" / "part-00000.parquet"
    )
    if not path.exists():
        raise FileNotFoundError(
            f"Nenhum bronze.estabelecimentos encontrado em {path}. Rode o grupo "
            f"'estabelecimentos' pelo menos uma vez para esta data_referencia/UF antes de "
            f"processar grupos que dependem de cnpj_basico (empresas, socios, simples)."
        )
    table = pq.read_table(path, columns=["cnpj_basico"])
    return set(table.column("cnpj_basico").to_pylist())


def transform(groups: list[str], reference_date: str) -> None:
    raw_dir = RAW_DIR / reference_date
    if not raw_dir.exists():
        raise FileNotFoundError(f"Nenhum dado baixado para {reference_date} em {raw_dir}")

    unknown = [g for g in groups if g not in GROUP_CONFIGS]
    if unknown:
        raise ValueError(f"Grupo(s) desconhecido(s): {unknown}. Disponíveis: {sorted(GROUP_CONFIGS)}")

    logger.info("Grupos: %s | UF=%s | data_referencia=%s", groups, UF_FILTRO, reference_date)

    # 1) Tabelas de domínio/referência — sem dependência, podem rodar em qualquer ordem.
    for group in groups:
        cfg = GROUP_CONFIGS[group]
        if cfg.filter_strategy == "none":
            _process_none(cfg, raw_dir, reference_date, _get_shards(group, cfg))

    # 2) Estabelecimentos — precisa rodar antes de qualquer grupo 'by_cnpj_basico'.
    matched_cnpj_basico: set[str] | None = None
    if "estabelecimentos" in groups:
        cfg = GROUP_CONFIGS["estabelecimentos"]
        matched_cnpj_basico = _process_own_uf(cfg, raw_dir, reference_date, _get_shards("estabelecimentos", cfg))

    # 3) Grupos que dependem do cnpj_basico filtrado (empresas, socios, simples).
    dependent_groups = [g for g in groups if GROUP_CONFIGS[g].filter_strategy == "by_cnpj_basico"]
    if dependent_groups:
        if matched_cnpj_basico is None:
            matched_cnpj_basico = _load_matched_cnpj_basico(reference_date)

        if not matched_cnpj_basico:
            logger.warning("Nenhum cnpj_basico para UF=%s — grupos dependentes não serão processados.", UF_FILTRO)
        else:
            for group in dependent_groups:
                cfg = GROUP_CONFIGS[group]
                _process_by_cnpj_basico(
                    cfg, raw_dir, reference_date, _get_shards(group, cfg), matched_cnpj_basico
                )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Transforma ZIPs baixados em Parquet Bronze filtrado por UF")
    parser.add_argument(
        "--groups",
        "-g",
        type=str,
        default="estabelecimentos,empresas",
        help=f"Grupos a processar, separados por vírgula. Disponíveis: {sorted(GROUP_CONFIGS)}",
    )
    parser.add_argument("--month", "-m", type=str, help="data_referencia (AAAA-MM). Default: mais recente em data/raw.")
    args = parser.parse_args()

    requested_groups = [g.strip() for g in args.groups.split(",") if g.strip()]

    if args.month:
        selected_reference_date = args.month
    else:
        available = sorted(p.name for p in RAW_DIR.iterdir() if p.is_dir())
        if not available:
            logger.error("Nenhuma pasta de data_referencia encontrada em %s", RAW_DIR)
            sys.exit(1)
        selected_reference_date = available[-1]

    transform(requested_groups, selected_reference_date)
