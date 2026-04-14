"""
load_staging.py — Load cleaned, typed data into the PostgreSQL staging schema.
                                                                                      
Reads from raw schema tables. For each table, applies:
- Column name normalization via schema_map.normalize_column                       
- Suppression code handling: reserve codes → NULL                                 
- Type inference: INTEGER for numeric columns, TEXT for everything else           
                                                                                      
Idempotent: drops and recreates staging tables on each run.                         
                                                                                      
Prerequisite: load_raw.py must have been run first for the target vintage(s).
                                                                                      
Usage:          
    python -m ingestion.load_staging --vintage 2021-22                              
    python -m ingestion.load_staging --all                                          
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

from ingestion.utils.schema_map import (                                            
    get_sch_dir, get_lea_dir, get_canonical_name, normalize_column,
  )                                                                                   
from ingestion.utils.suppression import handle_suppression
                                                                                      
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
                                                                                      
# Columns that are pure identifiers — can never contain reserve codes.              
# Passed through as TEXT with no suppression handling applied.
IDENTIFIER_COLS = frozenset({                                                       
    "combokey", "leaid", "schid",                                                   
    "lea_state", "lea_name", "sch_name", "statename",                               
    "jj",                                                                           
    "survey_year",                                                                
})                                                                                  
                  
                                                                                      
# ── Helpers ────────────────────────────────────────────────────────────────────
def get_db_connection():                                                            
    """Open and return a psycopg2 connection using credentials from .env."""
    return psycopg2.connect(                                                        
        host=os.environ["POSTGRES_HOST"],
        port=os.environ["POSTGRES_PORT"],                                           
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],                                           
        password=os.environ["POSTGRES_PASSWORD"],
    )                                                                               
                  

def ensure_staging_schema(conn):
    """Create the staging schema if it doesn't already exist."""
    with conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS staging;")                         
    conn.commit()
    logger.info("staging schema ready")                                             
                                                                                      

def get_survey_year(vintage: str) -> int:                                           
    if vintage not in SURVEY_YEARS:
        raise ValueError(
            f"Unknown vintage: {vintage!r}. Known vintages: {list(SURVEY_YEARS)}"
        )                                                                           
    return SURVEY_YEARS[vintage]
                                                                                      
                                                                                      
def _infer_col_type(series: pd.Series) -> str:
    """                                                                             
    Return 'INTEGER' if all non-None values are numeric, 'TEXT' otherwise.
                                                                                      
    Called after suppression handling, so values are Python int, None, or
    a non-numeric string (e.g. 'Yes', 'No'). Any string value means TEXT.           
    """                                                                             
    non_null = series.dropna()
    if non_null.empty:                                                              
        return "INTEGER"  # all-NULL column — default to INTEGER
    return "TEXT" if any(isinstance(v, str) for v in non_null) else "BIGINT"
                                                                                      
   
# ── Core load function ─────────────────────────────────────────────────────────   
def load_file(conn, csv_path: Path, vintage: str) -> None:
    """Read one raw table, apply cleaning, and write to staging."""
    survey_year = get_survey_year(vintage)                                          
    canonical = get_canonical_name(csv_path.stem, vintage)
    table_name = f"{canonical}_{survey_year}"                                       
                  
    logger.info(f"Staging {csv_path.name} → staging.{table_name}")                  
   
    # ── 1. Read from the raw table ─────────────────────────────────────────────   
    raw_query = sql.SQL("SELECT * FROM raw.{}").format(sql.Identifier(table_name))
    with conn.cursor() as cur:                                                      
        cur.execute(raw_query)
        col_names = [desc[0] for desc in cur.description]                           
        raw_rows = cur.fetchall()

    df = pd.DataFrame(raw_rows, columns=col_names)                                  
   
    # ── 2. Normalize column names ──────────────────────────────────────────────   
    df.columns = [
        col if col == "survey_year"
        else normalize_column(col, canonical, vintage).lower()
        for col in df.columns                                                       
    ]
    columns = list(df.columns)                                                      
                  
    # ── 3. Apply suppression handling to all non-identifier columns ────────────
    # handle_suppression returns (cleaned_value, is_suppressed, suppression_code).
    # We keep only the cleaned value: a Python int, None (suppressed), or a         
    # string (non-numeric fields like Yes/No indicators).
    #                                                                               
    # NOTE: Cascading total suppression is not implemented in this pass.
    # If a disaggregated value is suppressed (-11 in 2017-18/2020-21,
    # -12 in 2021-22), OCR's own processing may have already set the                
    # calculated total to the same reserve code. This will be verified
    # by spot checking after the first full load. If cascading is not               
    # already present in the raw data, it will be implemented here in
    # a subsequent pass.                                                            
    value_cols = [c for c in columns if c not in IDENTIFIER_COLS]
    for col in value_cols:
        df[col] = df[col].apply(lambda v: handle_suppression(v, vintage)[0])        

    # ── 4. Infer column types ──────────────────────────────────────────────────   
    col_types = {col: _infer_col_type(df[col]) for col in value_cols}
                                                                                      
    # ── 5. Build rows with Python-native types for psycopg2 ────────────────────  
    # itertuples() returns numpy types (float64, int64) which psycopg2 mishandles
    # for INTEGER columns. We convert each value explicitly at row-build time.          
    # staging_types covers all columns — value cols from col_types, identifiers         
    # hardcoded (survey_year is INTEGER; all others are TEXT).                          
    staging_types = {                                                                   
        col: (                                                                          
            "INTEGER" if col == "survey_year"                                           
            else "TEXT" if col in IDENTIFIER_COLS
            else col_types[col]                                                         
        )           
        for col in columns
    }

    def _to_py(v, col_type):                                                            
        try:
            if pd.isna(v):                                                              
                return None
        except (TypeError, ValueError):
            pass
        return int(v) if col_type in ("INTEGER", "BIGINT") else v

    col_lists = {col: df[col].tolist() for col in columns}                              
    rows = [
        tuple(_to_py(col_lists[col][i], staging_types[col]) for col in columns)         
        for i in range(len(df))
    ]
                                                                                      
    # ── 6. Build DDL and load into staging ─────────────────────────────────────
    col_defs = []                                                                   
    for col in columns:
        if col == "survey_year":
            col_defs.append(sql.SQL("survey_year INTEGER"))                         
        elif col in IDENTIFIER_COLS:
            col_defs.append(sql.SQL("{} TEXT").format(sql.Identifier(col)))         
        else:   
            col_defs.append(
                sql.SQL("{} {}").format(                                            
                    sql.Identifier(col),
                    sql.SQL(col_types[col]),                                        
                )
            )
              

    drop_stmt = sql.SQL("DROP TABLE IF EXISTS staging.{};").format(                 
        sql.Identifier(table_name)
    )                                                                               
    create_stmt = sql.SQL("CREATE TABLE staging.{} ({});").format(
        sql.Identifier(table_name),
        sql.SQL(", ").join(col_defs),
    )                                                                               
    insert_stmt = sql.SQL("INSERT INTO staging.{} ({}) VALUES %s").format(
        sql.Identifier(table_name),                                                 
        sql.SQL(", ").join(sql.Identifier(col) for col in columns),
    )

    with conn.cursor() as cur:
        cur.execute(drop_stmt)
        cur.execute(create_stmt)
        psycopg2.extras.execute_values(cur, insert_stmt, rows)
                                                                                      
    conn.commit()
    logger.info(f"  → {len(rows):,} rows loaded into staging.{table_name}")         
                  
                                                                                      
# ── Vintage loader ─────────────────────────────────────────────────────────────
def load_vintage(conn, vintage: str) -> None:                                       
    """Load all SCH and LEA files for a given vintage into staging."""
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
        description="Load cleaned, typed data into the PostgreSQL staging schema."  
    )
    group = parser.add_mutually_exclusive_group(required=True)                      
    group.add_argument(
        "--vintage",
        choices=VINTAGES,
        help="Stage a single vintage (e.g. --vintage 2021-22)",
    )                                                                               
    group.add_argument(
        "--all",                                                                    
        action="store_true",
        help="Stage all three vintages",
    )
    return parser.parse_args()
                                                                                      
   
def main():                                         
    load_dotenv()
    args = parse_args()
    vintages_to_load = VINTAGES if args.all else [args.vintage]

    conn = get_db_connection()
    try:
        ensure_staging_schema(conn)
        for vintage in vintages_to_load:                                            
            load_vintage(conn, vintage)
    finally:                                                                        
        conn.close()
        logger.info("Database connection closed.")


if __name__ == "__main__":
    main()