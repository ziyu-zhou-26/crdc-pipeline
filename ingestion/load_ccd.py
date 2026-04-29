"""
load_ccd.py — Load CCD school characteristics and CRDC-CCD crosswalk into staging.

Sources:
    data/ccd/ccd_schools_2021.csv, ccd_schools_2022.csv  → staging.ccd_schools_2021/2022
    data/raw/crosswalk/2020-21-appendix.xlsx             → staging.crdc_ccd_crosswalk
    data/raw/crosswalk/2021-22-appendix.xlsx             → staging.crdc_ccd_crosswalk

Idempotent: drops and recreates CCD school tables on each run.
Crosswalk table uses CREATE IF NOT EXISTS + DELETE per vintage for idempotency.

Usage:
    python -m ingestion.load_ccd
"""

import logging
import os
from pathlib import Path

import pandas as pd
import psycopg2
import psycopg2.extras
from psycopg2 import sql
from dotenv import load_dotenv

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# ── Constants ──────────────────────────────────────────────────────────────────
CCD_DIR = Path(__file__).parent.parent / "data" / "ccd"
CROSSWALK_DIR = Path(__file__).parent.parent / "data" / "raw" / "crosswalk"

SURVEY_YEARS = {"2020-21": 2021, "2021-22": 2022}

CROSSWALK_CONFIG = {
    "2021-22": {
        "tab": "K. CRDC and EDFacts Crosswalk",
        "ncessch_cols": ["NCES SCH 2021-22", "NCES SCH 2020-21", "NCES SCH 2019-20"],
    },
    "2020-21": {
        "tab": "M. CRDC and EDFacts Crosswalk",
        "ncessch_cols": ["NCESSCH 2020-21", "NCESSCH 2021-22", "NCESSCH 2019-20"],
    },
}

# ELSI missing-value sentinels → NULL
ELSI_MISSING = {"–", "†", "‡", "N/A", ""}

# Partial-string → canonical column name for CCD school files.
# Matched against ELSI column names which include year suffixes.
CCD_COLUMN_MAP = {
    "School Name":                         "school_name",
    "State Name":                          "state_name",
    "School ID (7-digit)":                 "nces_school_id_7",
    "School ID (12-digit)":                "ncessch",
    "Agency ID":                           "nces_lea_id",
    "School Type":                         "school_type",
    "Locale":                              "locale",
    "Title I Eligible":                    "title_i_eligible",
    "School Level":                        "school_level",
    "Total Students All Grades (Excludes": "total_enrollment",
    "Free and Reduced Lunch":              "frpl_count",
    "Pupil/Teacher Ratio":                 "pupil_teacher_ratio",
}


# ── Helpers ────────────────────────────────────────────────────────────────────
def get_db_connection():
    return psycopg2.connect(
        host=os.environ["POSTGRES_HOST"],
        port=os.environ["POSTGRES_PORT"],
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )


def ensure_staging_schema(conn):
    with conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS staging;")
    conn.commit()


def _rename_ccd_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Rename verbose ELSI column names to canonical names via partial matching."""
    rename = {}
    for col in df.columns:
        for key, canonical in CCD_COLUMN_MAP.items():
            if key in col:
                rename[col] = canonical
                break
    unmatched = [c for c in df.columns if c not in rename]
    if unmatched:
        logger.warning(f"Unmatched CCD columns (will be dropped): {unmatched}")
    renamed = df.rename(columns=rename)
    return renamed[[c for c in CCD_COLUMN_MAP.values() if c in renamed.columns]]


def _clean_elsi_value(v):
    """Convert ELSI missing sentinels to None; leave everything else as a string."""
    if pd.isna(v):
        return None
    s = str(v).strip()
    return None if s in ELSI_MISSING else s


def _to_numeric_or_none(v):
    """Convert a value to float if possible, otherwise None."""
    if v is None:
        return None
    try:
        return float(v)
    except (ValueError, TypeError):
        return None


# ── CCD school loader ──────────────────────────────────────────────────────────
def load_ccd_schools(conn, vintage: str) -> None:
    survey_year = SURVEY_YEARS[vintage]
    csv_path = CCD_DIR / f"ccd_schools_{survey_year}.csv"
    table_name = f"ccd_schools_{survey_year}"

    logger.info(f"Loading {csv_path.name} → staging.{table_name}")

    df = pd.read_csv(csv_path, dtype=str, encoding="utf-8-sig")

    # Rename and drop unneeded ELSI columns
    df = _rename_ccd_columns(df)

    # Add survey_year
    df["survey_year"] = survey_year

    # Clean missing sentinels
    for col in df.columns:
        if col != "survey_year":
            df[col] = df[col].apply(_clean_elsi_value)

    # Convert numeric columns from string to float
    for col in ("frpl_count", "total_enrollment", "pupil_teacher_ratio"):
        if col in df.columns:
            df[col] = df[col].apply(_to_numeric_or_none)

    # Build rows
    columns = list(df.columns)
    rows = [
        tuple(None if pd.isna(v) else v for v in row)
        for row in df.itertuples(index=False, name=None)
    ]

    # DDL
    col_defs = []
    numeric_cols = {"frpl_count", "total_enrollment", "pupil_teacher_ratio"}
    for col in columns:
        if col == "survey_year":
            col_defs.append(sql.SQL("survey_year INTEGER"))
        elif col in numeric_cols:
            col_defs.append(sql.SQL("{} NUMERIC").format(sql.Identifier(col)))
        else:
            col_defs.append(sql.SQL("{} TEXT").format(sql.Identifier(col)))

    drop_stmt = sql.SQL("DROP TABLE IF EXISTS staging.{}").format(
        sql.Identifier(table_name)
    )
    create_stmt = sql.SQL("CREATE TABLE staging.{} ({})").format(
        sql.Identifier(table_name),
        sql.SQL(", ").join(col_defs),
    )
    insert_stmt = sql.SQL("INSERT INTO staging.{} ({}) VALUES %s").format(
        sql.Identifier(table_name),
        sql.SQL(", ").join(sql.Identifier(c) for c in columns),
    )

    with conn.cursor() as cur:
        cur.execute(drop_stmt)
        cur.execute(create_stmt)
        psycopg2.extras.execute_values(cur, insert_stmt, rows)
    conn.commit()
    logger.info(f"  → {len(rows):,} rows loaded into staging.{table_name}")


# ── Crosswalk loader ───────────────────────────────────────────────────────────
def load_crosswalk(conn, vintage: str) -> None:
    survey_year = SURVEY_YEARS[vintage]
    config = CROSSWALK_CONFIG[vintage]
    xlsx_path = CROSSWALK_DIR / f"{vintage}-appendix.xlsx"

    if not xlsx_path.exists():
        logger.error(f"Crosswalk file not found: {xlsx_path}. Skipping {vintage}.")
        return

    logger.info(
        f"Loading crosswalk from {xlsx_path.name} "
        f"(tab: {config['tab']}) → staging.crdc_ccd_crosswalk"
    )

    df = pd.read_excel(xlsx_path, sheet_name=config["tab"], dtype=str)
    df.columns = df.columns.str.replace(r"\s+", " ", regex=True).str.strip()

    # Keep only the columns we need
    ncessch_cols = config["ncessch_cols"]
    df = df[["COMBOKEY"] + ncessch_cols + ["MATCHED", "Comment"]].copy()
    
    # Resolve best available NCESSCH — primary year first, then fallbacks
    df["ncessch"] = df[ncessch_cols[0]]
    for fallback in ncessch_cols[1:]:
        df["ncessch"] = df["ncessch"].fillna(df[fallback])
    
    df = df[["COMBOKEY", "ncessch", "MATCHED", "Comment"]].copy()
    df.columns = ["combokey", "ncessch", "matched", "comment"]

    # Filter: matched only, exclude M:1
    before = len(df)
    df = df[df["matched"].str.upper() == "TRUE"]
    df = df[df["comment"].fillna("").str.strip() != "M:1"]
    logger.info(f"  Crosswalk {vintage}: {before:,} rows → {len(df):,} after filtering")

    df["survey_year"] = survey_year
    df = df[["combokey", "ncessch", "survey_year"]]

    rows = list(df.itertuples(index=False, name=None))

    with conn.cursor() as cur:
        # Create table once if it doesn't exist — no-op on second vintage
        cur.execute("""
            CREATE TABLE IF NOT EXISTS staging.crdc_ccd_crosswalk (
                combokey    TEXT,
                ncessch     TEXT,
                survey_year INTEGER
            )
        """)
        # Delete only this vintage's rows before reinserting — preserves other vintages
        cur.execute(
            "DELETE FROM staging.crdc_ccd_crosswalk WHERE survey_year = %s",
            (survey_year,)
        )
        psycopg2.extras.execute_values(
            cur,
            "INSERT INTO staging.crdc_ccd_crosswalk "
            "(combokey, ncessch, survey_year) VALUES %s",
            rows,
        )
    conn.commit()
    logger.info(
        f"  → {len(rows):,} rows loaded into staging.crdc_ccd_crosswalk "
        f"for survey_year={survey_year}"
    )


# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    load_dotenv()
    conn = get_db_connection()
    try:
        ensure_staging_schema(conn)
        for vintage in ("2020-21", "2021-22"):
            load_ccd_schools(conn, vintage)
            load_crosswalk(conn, vintage)
    finally:
        conn.close()
        logger.info("Database connection closed.")


if __name__ == "__main__":
    main()