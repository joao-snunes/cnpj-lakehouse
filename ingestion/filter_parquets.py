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



def _upload_parquet(relations: dict[str, duckdb.DuckDBPyRelation]):
     ...
     

def filter_parquets(dir: Path, uf: str | None = None, reference_date: str | None = None):

     sharded_files = ["estabelecimentos", "empresas", "socios"]
     unsharded_files = ["simples", "dim_cnae", "dim_municipio", "dim_natureza_juridica", "dim_qualificacao_socio", "dim_motivo_deativacao", "dim_pais"]
     paths = {}
     stg_relations = {}
     
     for table in sharded_files:
          paths[table] = f"{dir}/{table}/data_referencia=*/shard=*/*.parquet"
     for table in unsharded_files:
          paths[table] = f"{dir}/{table}/data_referencia=*/*.parquet"
     
     for table, path in paths.items():
          reader = ddb.read_parquet(path, hive_partitioning=True) 
          
          if reference_date is not None:
               reader = reader.filter(f"data_referencia = '{reference_date}'")
          if table == "estabelecimentos":
               reader = reader.filter(f"uf = '{uf}'")
               
          stg_relations[table] = reader
          reader.to_view(table + "_stg")
               
     ddb.sql("""
             CREATE TEMP TABLE cnpj_elegiveis AS 
             SELECT DISTINCT cnpj_basico
             FROM estabelecimentos_stg
             """)
     
     ddb.sql("""
             COPY (
                  SELECT * FROM estabelecimentos_stg 
             ) TO S3::...
             """)
     
     ddb.sql("""
             COPY (
                  SELECT * FROM empresas_stg
                  SEMI JOIN cnpj_elegiveis USING (cnpj_basico)
             ) TO S3::...
             """)