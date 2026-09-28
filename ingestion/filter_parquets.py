import duckdb 
import logging
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)
ddb = duckdb.connect()

RAW_DIR = Path("data/raw")
STAGING_DIR = Path("data/staging")
UF = os.getenv("UF_FILTRO")
AWS_ACCESS_KEY_ID= os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY= os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION")

ddb.sql(
     f"""
     INSTALL httpfs;
     LOAD httpfs;
     
     CREATE SECRET (
          TYPE s3,
          KEY_ID '{AWS_ACCESS_KEY_ID}',
          SECRET '{AWS_SECRET_ACCESS_KEY}',
          REGION '{AWS_DEFAULT_REGION}'
     );
     """
)

def _upload_parquet(tables: list[duckdb.DuckDBPyRelation]):
     ...
     

def filter_parquets(dir: Path, uf: str | None = None, reference_date: str | None = None, shard: str | None = None):
     estab_dir = f"{dir}/estabelecimentos/data_referencia=*/shard=*/*.parquet"
     estab = ddb.read_parquet(estab_dir, hive_partitioning=True)
     
     if reference_date is not None:
          estab = estab.filter(f"data_referencia = {reference_date}")
     if shard is not None:
          estab = estab.filter(f"shard = {shard}")
     

     
     