"""                                                                                 
load_mart_bq.py - Export PostgreSQL mart tables to BigQuery.                        
                                                                                      
Reads all mart tables from PostgreSQL and loads them into the                       
BigQuery dataset specified by BQ_PROJECT and BQ_DATASET.                            
                                                                                      
Idempotent: replaces existing BigQuery tables on each run.
                                                                                      
Usage:                                  
    python -m ingestion.load_mart_bq
    python -m ingestion.load_mart_bq --table <table_name>
"""                                                                                 
  
import logging
import os
import argparse
                                              
import pandas as pd                     
import psycopg2
from dotenv import load_dotenv                                                      
from google.cloud import bigquery
                                                                                      
load_dotenv()   

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",                 
)
logger = logging.getLogger(__name__)                                                
  
BQ_PROJECT = "crdc-26"                                                              
BQ_DATASET = "mart"                     

MART_TABLES = [
    "dim_year",
    "dim_district",
    "dim_school",
    "fct_enrollment_k12",
    "fct_enrollment_ps",
    "fct_enrollment_k12_programs",
    "fct_enrollment_ps_programs",
    "fct_internet",
    "agg_internet_by_state",                
  ]
                                                                                     
def get_pg_conn():                                                                  
    return psycopg2.connect(
        host=os.environ["POSTGRES_HOST"],                                                 
        port=os.environ.get("POSTGRES_PORT", 5432),
        dbname=os.environ["POSTGRES_DB"],         
        user=os.environ["POSTGRES_USER"],     
        password=os.environ["POSTGRES_PASSWORD"],
    )                                                                               
                                              
                                                                                      
def load_table(pg_conn, bq_client, table_name):
    logger.info(f"Reading {table_name} from PostgreSQL...")                         
    df = pd.read_sql(f"SELECT * FROM mart.{table_name}", pg_conn)
    logger.info(f"  {len(df):,} rows read")                                         
                                              
    dest = f"{BQ_PROJECT}.{BQ_DATASET}.{table_name}"                                
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,                 
    )                                                                               
                                                                                      
    logger.info(f"  Loading to BigQuery: {dest}...")                                
    job = bq_client.load_table_from_dataframe(df, dest, job_config=job_config)
    job.result()                            
    logger.info(f"  Done.")                                                         
  
                                                                                      
def main():                                                           
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--table",
        help="Load a single table by name. If omitted, loads all tables.",
    )
    args = parser.parse_args()
    
    tables = [args.table] if args.table else MART_TABLES
    
    pg_conn = get_pg_conn()
    bq_client = bigquery.Client(project=BQ_PROJECT)
    
    for table in tables:
        load_table(pg_conn, bq_client, table)

    pg_conn.close()
    logger.info("All tables loaded successfully.")
                                  
if __name__ == "__main__":
    main()  