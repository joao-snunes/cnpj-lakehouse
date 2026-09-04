"""
Download dos arquivos públicos de CNPJ da Receita Federal, hospedados em um
compartilhamento público Nextcloud.

Repositório de referência:
- https://github.com/caiopizzol/cnpj-data-pipeline

Mecanismo Nextcloud:
  URL de compartilhamento: https://<host>/index.php/s/<token>?dir=<path>
  Endpoint WebDAV:         https://<host>/public.php/webdav/<path>
  Autenticação:            Basic Auth, usuário = token, senha = "" (vazia)
  Listagem de diretório:   método HTTP PROPFIND (Depth: 1), resposta em XML

Este script cobre apenas listagem + download, sendo uma simplificação do cnpj-data-pipeline.
"""

import os
import re
import logging
from pathlib import Path
from xml.etree import ElementTree

import requests
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# --- Configuração do compartilhamento Nextcloud da Receita Federal ---

BASE_URL = os.getenv("BASE_URL", "https://arquivos.receitafederal.gov.br/public.php/webdav")
SHARE_TOKEN = os.getenv("SHARE_TOKEN", "YggdBLfdninEJX9")
AUTH = (SHARE_TOKEN, "")

DAV_NS = {"d": "DAV:"}

# Grupos de arquivo disponíveis e o padrão usado para identificá-los no
# nome do arquivo .zip listado via WebDAV.
FILE_GROUPS = {
    "empresas": re.compile(r"Empresas\d*\.zip", re.IGNORECASE),
    "estabelecimentos": re.compile(r"Estabelecimentos\d*\.zip", re.IGNORECASE),
    "socios": re.compile(r"Socios\d*\.zip", re.IGNORECASE),
    "simples": re.compile(r"Simples\.zip", re.IGNORECASE),
    "cnaes": re.compile(r"Cnaes\.zip", re.IGNORECASE),
    "municipios": re.compile(r"Municipios\.zip", re.IGNORECASE),
    "naturezas": re.compile(r"Naturezas\.zip", re.IGNORECASE),
    "qualificacoes": re.compile(r"Qualificacoes\.zip", re.IGNORECASE),
    "motivos": re.compile(r"Motivos\.zip", re.IGNORECASE),
    "paises": re.compile(r"Paises\.zip", re.IGNORECASE),
}


def _propfind(path: str = "") -> ElementTree.Element:
    """Executa um PROPFIND WebDAV e retorna o XML de resposta parseado."""
    url = f"{BASE_URL}/{path}".rstrip("/") + "/"
    response = requests.request(
        "PROPFIND",
        url,
        auth=AUTH,
        headers={"Depth": "1"},
        timeout=30,
    )
    response.raise_for_status()
    return ElementTree.fromstring(response.content)


def get_available_reference_dates() -> list[str]:
    """Lista os diretórios de data de referência (formato AAAA-MM) disponíveis."""
    root = _propfind()

    dates = []
    for response in root.findall("d:response", DAV_NS):
        href = response.find("d:href", DAV_NS).text
        match = re.search(r"(\d{4}-\d{2})/?$", href)
        if match:
            dates.append(match.group(1))

    if not dates:
        raise ValueError("Nenhum diretório de data de referência encontrado.")

    return sorted(dates)


def get_latest_reference_date() -> str:
    """Retorna a data de referência mais recente disponível."""
    latest = get_available_reference_dates()[-1]
    logger.info("Última data de referência disponível: %s", latest)
    return latest


def list_files_for_reference_date(reference_date: str) -> list[str]:
    """Lista todos os arquivos .zip disponíveis para uma data de referência específica."""
    root = _propfind(reference_date)

    files = []
    for response in root.findall("d:response", DAV_NS):
        href = response.find("d:href", DAV_NS).text
        match = re.search(r"/([^/]+\.zip)$", href, re.IGNORECASE)
        if match:
            files.append(match.group(1))

    logger.info("Encontrados %d arquivos .zip em %s", len(files), reference_date)
    return files


def filter_files_by_groups(files: list[str], groups: list[str]) -> list[str]:
    """Filtra a lista de arquivos apenas pelos grupos desejados (ex: ['empresas', 'estabelecimentos'])."""
    patterns = [FILE_GROUPS[g] for g in groups if g in FILE_GROUPS]
    filtered = [f for f in files if any(p.search(f) for p in patterns)]
    logger.info("Arquivos filtrados para os grupos %s: %d de %d", groups, len(filtered), len(files))
    return filtered


def download_file(reference_date: str, filename: str, destination_dir: Path, chunk_size: int = 1024 * 1024) -> Path:
    """Baixa um único arquivo em streaming (evita carregar o arquivo inteiro na memória)."""
    destination_dir.mkdir(parents=True, exist_ok=True)
    destination_path = destination_dir / filename

    if destination_path.exists():
        logger.info("Arquivo já existe, pulando download: %s", filename)
        return destination_path

    url = f"{BASE_URL}/{reference_date}/{filename}"
    logger.info("Baixando %s ...", filename)

    with requests.get(url, auth=AUTH, stream=True, timeout=120) as response:
        response.raise_for_status()
        with open(destination_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=chunk_size):
                f.write(chunk)

    logger.info("Download concluído: %s", destination_path)
    return destination_path


def download_reference_date(reference_date: str, groups: list[str], output_dir: str = "data/raw") -> list[Path]:
    """
    Baixa todos os arquivos dos grupos especificados para uma data de referência,
    organizando localmente em output_dir/<reference_date>/.
    """
    files = list_files_for_reference_date(reference_date)
    files = filter_files_by_groups(files, groups)

    destination_dir = Path(output_dir) / reference_date
    downloaded = [download_file(reference_date, filename, destination_dir) for filename in files]
    return downloaded


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Download dos arquivos de CNPJ da Receita Federal")
    parser.add_argument(
        "--groups",
        "-g",
        type=str,
        default="empresas,estabelecimentos",
        help=f"Grupos a baixar, separados por vírgula. Disponíveis: {sorted(FILE_GROUPS)}",
    )
    parser.add_argument(
        "--month",
        "-m",
        type=str,
        help="Data de referência (AAAA-MM). Default: mais recente disponível.",
    )
    args = parser.parse_args()

    GROUPS = [g.strip() for g in args.groups.split(",") if g.strip()]
    unknown = [g for g in GROUPS if g not in FILE_GROUPS]
    if unknown:
        raise SystemExit(f"Grupo(s) desconhecido(s): {unknown}. Disponíveis: {sorted(FILE_GROUPS)}")

    reference_date = args.month or get_latest_reference_date()
    downloaded_files = download_reference_date(reference_date, GROUPS)

    logger.info("Total de arquivos baixados: %d", len(downloaded_files))
    for f in downloaded_files:
        logger.info(" - %s", f)