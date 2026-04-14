"""                                                                                
load_raw.py — Load CRDC SCH and LEA CSV files into the PostgreSQL raw schema.
                                                                                     
One table per file per vintage. No transformations. All columns stored as TEXT.    
Adds a survey_year INTEGER column to every row.                                    
Idempotent: drops and recreates tables on each run.                                
                                                                                     
Usage:                                                                             
    python -m ingestion.load_raw --vintage 2021-22                                 
    python -m ingestion.load_raw --all
"""

import argparse                                                                    
import logging
import os                                                                          
from pathlib import Path

import pandas as pd
import psycopg2
import psycopg2.extras
from psycopg2 import sql
from dotenv import load_dotenv                                                     
   
from ingestion.utils.schema_map import get_sch_dir, get_lea_dir, get_canonical_name
                  
# ── Logging ────────────────────────────────────────────────────────────────────  
logging.basicConfig(
    level=logging.INFO,                                                            
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)                                                                                  
logger = logging.getLogger(__name__)
                                                                                     
# ── Constants ──────────────────────────────────────────────────────────────────
DATA_DIR = Path(__file__).parent.parent / "data" / "raw"
VINTAGES = ["2017-18", "2020-21", "2021-22"]                                       
   
SURVEY_YEARS = {                                                                   
    "2017-18": 2018,
    "2020-21": 2021,                                                               
    "2021-22": 2022,
}

                                                                                     
# ── Helpers ────────────────────────────────────────────────────────────────────                                                                     
def get_survey_year(vintage: str) -> int:
    """Return the survey year integer for a vintage string.
                                                                                     
    Convention: the ending year of the vintage range.
    e.g. "2020-21" → 2021, "2021-22" → 2022, "2017-18" → 2018                      
    """                                                                            
    if vintage not in SURVEY_YEARS:
        raise ValueError(                                                          
            f"Unknown vintage: {vintage!r}. "
            f"Known vintages: {list(SURVEY_YEARS)}"                                
        )
    return SURVEY_YEARS[vintage]
                                                                                    
  
def get_db_connection():                                                           
    """Open and return a psycopg2 connection using credentials from .env."""
    return psycopg2.connect(                                                       
        host=os.environ["POSTGRES_HOST"],
        port=os.environ["POSTGRES_PORT"],                                                
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],                                                
        password=os.environ["POSTGRES_PASSWORD"],
    )                                                                              
  

def ensure_raw_schema(conn):
    """Create the raw schema if it doesn't already exist."""
    with conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS raw;")                            
    conn.commit()
    logger.info("raw schema ready")                                                
                                                                                     
   
# ── Core load function ─────────────────────────────────────────────────────────                 
def load_file(conn, csv_path: Path, vintage: str) -> None:                         
    """Load a single CSV file into the raw schema."""
    survey_year = get_survey_year(vintage)                                         
    canonical = get_canonical_name(csv_path.stem, vintage)
    table_name = f"{canonical}_{survey_year}"
                                                                                     
    logger.info(f"Loading {csv_path.name} → raw.{table_name}")
                                                                                     
    # dtype=str: read every column as a string — no type coercion in the raw layer
    # keep_default_na=False: prevent pandas from converting strings like "NA"
    # to NaN, which would corrupt school names and other text fields               
    try:
        df = pd.read_csv(csv_path, dtype=str, keep_default_na=False, encoding='utf-8-sig')
    except UnicodeDecodeError:
        logger.warning(f"UTF-8 decode failed for {csv_path.name}, retrying with latin-1")
        df = pd.read_csv(csv_path, dtype=str, keep_default_na=False, encoding='latin-1')
                                                                                     
    # Strip whitespace and stray quote characters from column headers.             
    # Defensive measure applied universally — the 2017-18 Calculus.csv file
    # has a malformed header with a leading space and surrounding quotes.          
    df.columns = [col.strip().strip('"').strip() for col in df.columns]
                                                                                     
    df["survey_year"] = survey_year
                                                                                     
    columns = list(df.columns)

    # Build column definitions for CREATE TABLE.
    # All source columns are TEXT. survey_year is INTEGER (pipeline-added metadata).
    # psycopg2.sql.Identifier safely quotes column names as identifiers.
    col_defs = []                                                                  
    for col in columns:
        if col == "survey_year":                                                   
            col_defs.append(sql.SQL("survey_year INTEGER"))
        else:
            col_defs.append(sql.SQL("{} TEXT").format(sql.Identifier(col)))
                                                                                     
    drop_stmt = sql.SQL("DROP TABLE IF EXISTS raw.{};").format(
        sql.Identifier(table_name)                                                 
    )           
    create_stmt = sql.SQL("CREATE TABLE raw.{} ({});").format(
        sql.Identifier(table_name),                                                
        sql.SQL(", ").join(col_defs),
    )                                                                              
    insert_stmt = sql.SQL("INSERT INTO raw.{} ({}) VALUES %s").format(
        sql.Identifier(table_name),                                                
        sql.SQL(", ").join(sql.Identifier(col) for col in columns),
    )                                                                       
               
    rows = [tuple(row) for row in df.itertuples(index=False, name=None)]           
   
    with conn.cursor() as cur:                                                     
        cur.execute(drop_stmt)
        cur.execute(create_stmt)
        psycopg2.extras.execute_values(cur, insert_stmt, rows)

    conn.commit()
    logger.info(f"  → {len(rows):,} rows loaded into raw.{table_name}")
                                                                                     
   
# ── Vintage loader ─────────────────────────────────────────────────────────────
def load_vintage(conn, vintage: str) -> None:
    """Load all SCH and LEA CSV files for a given vintage."""
    sch_dir = get_sch_dir(vintage, DATA_DIR)                                       
    lea_dir = get_lea_dir(vintage, DATA_DIR)
                                                                                     
    for label, directory in [("SCH", sch_dir), ("LEA", lea_dir)]:                  
        csv_files = sorted(directory.glob("*.csv"))
        if not csv_files:                                                          
            logger.warning(f"No CSV files found in {directory}")
            continue
        logger.info(f"=== {vintage} {label} ({len(csv_files)} files) ===")
        for csv_path in csv_files:                                                 
            load_file(conn, csv_path, vintage)
                                                                                     
    logger.info(f"=== Finished {vintage} ===")                                     
   
                                                                                     
# ── CLI ────────────────────────────────────────────────────────────────────────
def parse_args():
    parser = argparse.ArgumentParser(
        description="Load CRDC SCH and LEA CSV files into the PostgreSQL raw schema."                                                                           
    )
    group = parser.add_mutually_exclusive_group(required=True)                     
    group.add_argument(
        "--vintage",
        choices=VINTAGES,
        help="Load a single vintage (e.g. --vintage 2021-22)",
    )                                                                              
    group.add_argument(
        "--all",                                                                   
        action="store_true",
        help="Load all three vintages",
    )
    return parser.parse_args()


def main():
    load_dotenv()
    args = parse_args()
    vintages_to_load = VINTAGES if args.all else [args.vintage]
                                                                                     
    conn = get_db_connection()
    try:                                                                           
        ensure_raw_schema(conn)
        for vintage in vintages_to_load:
            load_vintage(conn, vintage)
    finally:                                                                       
        conn.close()
        logger.info("Database connection closed.")                                 
                  
if __name__ == "__main__":
    main()