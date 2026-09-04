"""
Extrai ZIPs da Receita Federal já baixados por download.py e grava Parquet
para a camada de staging — sem filtro de UF, sem join por cnpj_basico.

Lê de data/raw/<data_referencia>/ e grava em:

    data/staging/<tabela>/data_referencia=<AAAA-MM>/shard=<N>/part-00000.parquet   (grupos fragmentados)
    data/staging/<tabela>/data_referencia=<AAAA-MM>/part-00000.parquet            (grupos de arquivo único)

Repo de referencia: github.com/caiopizzol/cnpj-data-pipeline (processor.py)
"""

import argparse
import io
import logging
import os
import sys
import zipfile
from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow import csv as pacsv
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

RAW_DIR = Path("data/raw")
STAGING_DIR = Path("data/staging")
CSV_BLOCK_SIZE = 32 << 20  # 32MB por bloco de leitura/transcodificação


@dataclass(frozen=True)
class GroupConfig:
    zip_prefix: str  # nome do zip é f"{zip_prefix}{shard}.zip" (shard='' se não fragmentado)
    member_pattern: str  # substring (maiúscula) do nome do CSV dentro do zip
    columns: list[str]  # layout oficial, na ordem, para o CSV sem header
    table_name: str  # nome da tabela de saída na staging
    sharded: bool  # True: até 10 arquivos numerados 0..9; False: arquivo único

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

# um grupo por entrada de FILE_GROUPS em download.py. Adicionar um grupo novo
# implica em mais uma entrada aqui.
GROUP_CONFIGS: dict[str, GroupConfig] = {
    "estabelecimentos": GroupConfig("Estabelecimentos", "ESTABELE", _ESTABELECIMENTOS_COLUMNS, "estabelecimentos", True),
    "empresas": GroupConfig("Empresas", "EMPRECSV", _EMPRESAS_COLUMNS, "empresas", True),
    "socios": GroupConfig("Socios", "SOCIOCSV", _SOCIOS_COLUMNS, "socios", True),
    "simples": GroupConfig("Simples", "SIMPLES", _SIMPLES_COLUMNS, "simples", False),
    "cnaes": GroupConfig("Cnaes", "CNAECSV", _DOMINIO_COLUMNS, "dim_cnae", False),
    "municipios": GroupConfig("Municipios", "MUNICCSV", _DOMINIO_COLUMNS, "dim_municipio", False),
    "naturezas": GroupConfig("Naturezas", "NATJUCSV", _DOMINIO_COLUMNS, "dim_natureza_juridica", False),
    "qualificacoes": GroupConfig("Qualificacoes", "QUALSCSV", _DOMINIO_COLUMNS, "dim_qualificacao_socio", False),
    "motivos": GroupConfig("Motivos", "MOTICSV", _DOMINIO_COLUMNS, "dim_motivo_deativacao", False),
    "paises": GroupConfig("Paises", "PAISCSV", _DOMINIO_COLUMNS, "dim_pais", False),
}


def _get_shards(group: str, cfg: GroupConfig, shards_override: list[str] | None) -> list[str]:
    """Shards a processar para um grupo. Grupos não fragmentados sempre retornam [''],
    resultando no nome de arquivo f"{zip_prefix}.zip". Prioridade: --shards do CLI >
    env var SHARDS_<GROUP> > todos os 10 shards."""
    if not cfg.sharded:
        return [""]
    if shards_override is not None:
        return shards_override
    env_var = f"SHARDS_{group.upper()}"
    default = ",".join(str(i) for i in range(10))
    return [s.strip() for s in os.getenv(env_var, default).split(",") if s.strip()]


def _find_zip_member(zip_ref: zipfile.ZipFile, pattern: str) -> str:
    for name in zip_ref.namelist():
        if pattern in name.upper():
            return name
    raise FileNotFoundError(f"Nenhum membro contendo '{pattern}' em {zip_ref.filename}")


class _Latin1ToUtf8Transcoder(io.RawIOBase):
    """Envolve um stream de bytes latin-1 e o expõe como bytes utf-8, decodificando
    bloco a bloco.
    """

    def __init__(self, raw, block_size: int = CSV_BLOCK_SIZE):
        self._raw = raw
        self._block_size = block_size
        self._buf = b""

    def readable(self) -> bool:
        return True

    def readinto(self, b) -> int:
        if not self._buf:
            chunk = self._raw.read(self._block_size)
            if not chunk:
                return 0
            self._buf = chunk.decode("latin-1").encode("utf-8")
        n = min(len(b), len(self._buf))
        b[:n] = self._buf[:n]
        self._buf = self._buf[n:]
        return n


def _read_csv_batches(zip_path: Path, member_pattern: str, columns: list[str]):
    """Itera em batches um CSV sem header, separado por ';', latin-1 de
    dentro do ZIP (sem extrair para disco).
    """
    with zipfile.ZipFile(zip_path) as zip_ref:
        member = _find_zip_member(zip_ref, member_pattern)
        with zip_ref.open(member) as raw:
            binary_stream = io.BufferedReader(_Latin1ToUtf8Transcoder(raw), buffer_size=CSV_BLOCK_SIZE)
            reader = pacsv.open_csv(
                binary_stream,
                read_options=pacsv.ReadOptions(column_names=columns, block_size=CSV_BLOCK_SIZE),
                parse_options=pacsv.ParseOptions(delimiter=";"),
                convert_options=pacsv.ConvertOptions(column_types={c: pa.string() for c in columns}),
            )
            for batch in reader:
                yield pa.Table.from_batches([batch])


class ParquetWriter:
    """Escreve batches de Arrow Table em um único arquivo Parquet por shard,
    particionado por data_referencia (+ shard, quando aplicável) via diretórios."""

    def __init__(self, output_dir: Path, table_name: str, reference_date: str, shard: str | None):
        out_dir = output_dir / table_name / f"data_referencia={reference_date}"
        if shard is not None:
            out_dir = out_dir / f"shard={shard}"
        out_dir.mkdir(parents=True, exist_ok=True)
        self.path = out_dir / "part-00000.parquet"
        self._writer: pq.ParquetWriter | None = None
        self.rows = 0

    def write(self, table: pa.Table) -> None:
        if table.num_rows == 0:
            return
        if self._writer is None:
            self._writer = pq.ParquetWriter(str(self.path), table.schema, compression="snappy")
        self._writer.write_table(table)
        self.rows += table.num_rows

    def close(self) -> None:
        if self._writer is not None:
            self._writer.close()


def _process_shard(
    cfg: GroupConfig, zip_path: Path, output_dir: Path, reference_date: str, shard: str
) -> None:
    logger.info("Lendo %s", zip_path.name)
    writer = ParquetWriter(output_dir, cfg.table_name, reference_date, shard if cfg.sharded else None)

    for table in _read_csv_batches(zip_path, cfg.member_pattern, cfg.columns):
        reference_col = pa.array([reference_date] * table.num_rows, type=pa.string())
        table = table.append_column("data_referencia", reference_col)
        writer.write(table)

    writer.close()
    logger.info("%s (shard=%s): %d linhas gravadas em %s", cfg.table_name, shard or "-", writer.rows, writer.path)


def extract(groups: list[str], reference_date: str, output_dir: Path, shards_override: list[str] | None) -> None:
    raw_dir = RAW_DIR / reference_date
    if not raw_dir.exists():
        raise FileNotFoundError(f"Nenhum dado baixado para {reference_date} em {raw_dir}")

    unknown = [g for g in groups if g not in GROUP_CONFIGS]
    if unknown:
        raise ValueError(f"Grupo(s) desconhecido(s): {unknown}. Disponíveis: {sorted(GROUP_CONFIGS)}")

    logger.info("Grupos: %s | data_referencia=%s | saída=%s", groups, reference_date, output_dir)

    for group in groups:
        cfg = GROUP_CONFIGS[group]
        for shard in _get_shards(group, cfg, shards_override):
            zip_path = raw_dir / f"{cfg.zip_prefix}{shard}.zip"
            if not zip_path.exists():
                logger.warning("Shard não encontrado, pulando: %s", zip_path)
                continue
            _process_shard(cfg, zip_path, output_dir, reference_date, shard)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extrai ZIPs baixados para Parquet de staging (sem filtro/join)")
    parser.add_argument(
        "--groups",
        "-g",
        type=str,
        default="estabelecimentos,empresas",
        help=f"Grupos a processar, separados por vírgula. Disponíveis: {sorted(GROUP_CONFIGS)}",
    )
    parser.add_argument("--month", "-m", type=str, help="data_referencia (AAAA-MM). Default: mais recente em data/raw.")
    parser.add_argument(
        "--shards",
        type=str,
        help="Shards a extrair, separados por vírgula (ex: '0,3,7'). Default: env var SHARDS_<GRUPO> ou todos (0-9).",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=str(STAGING_DIR),
        help=f"Diretório de saída da staging. Default: {STAGING_DIR}",
    )
    args = parser.parse_args()

    requested_groups = [g.strip() for g in args.groups.split(",") if g.strip()]
    requested_shards = [s.strip() for s in args.shards.split(",") if s.strip()] if args.shards else None

    if args.month:
        selected_reference_date = args.month
    else:
        available = sorted(p.name for p in RAW_DIR.iterdir() if p.is_dir())
        if not available:
            logger.error("Nenhuma pasta de data_referencia encontrada em %s", RAW_DIR)
            sys.exit(1)
        selected_reference_date = available[-1]

    extract(requested_groups, selected_reference_date, Path(args.output_dir), requested_shards)
