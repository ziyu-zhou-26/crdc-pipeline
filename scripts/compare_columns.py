"""
scripts/compare_columns.py

Standalone exploratory script — NOT part of the pipeline.
Produces a column-level comparison across all three CRDC SCH vintages.

Outputs (saved to docs/column_comparison/):
  full_column_inventory.csv  — one row per (canonical_file, column, vintage)
  column_diff_report.csv     — discrepancies only: columns missing from one or more vintages
  summary.txt                — file counts, missing files, top discrepancy files

Run from the project root with the venv activated:
  python scripts/compare_columns.py

NOTE: compares raw column names as they appear in the files.
No column renames are applied — the point is to discover what renames are needed.
"""

import csv
import re
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data" / "raw"
OUT_DIR = ROOT / "docs" / "column_comparison"

VINTAGES = ["2017-18", "2020-21", "2021-22"]

SCH_DIRS = {
    "2021-22": DATA_DIR / "2021-22" / "SCH",
    "2020-21": DATA_DIR / "2020-21" / "CRDC" / "School",
    "2017-18": DATA_DIR / "2017-18" / "2017-18-crdc-data-corrected-publication 2" / "2017-18 Public-Use Files" / "Data" / "SCH" / "CRDC" / "CSV",
}

# Known file name overrides: {vintage: {raw_stem: canonical_name}}
# For everything else, canonical name = slugify(stem).
FILE_OVERRIDES = {
    "2021-22": {
        "Interscholastic Athletics": "athletics",
        "Single Sex Classes":        "single_sex_classes",
        "High School Equivalency Exam": "hs_equivalency",
    },
    "2020-21": {
        "Single sex Athletics": "athletics",
        "Single sex Classes":   "single_sex_classes",
        "High School Equivalency": "hs_equivalency",
    },
    "2017-18": {
        "Single-sex Athletics": "athletics",
        "Single-sex Classes":   "single_sex_classes",
        "High School Equivalency (GED)": "hs_equivalency",
    },
}

# Known intentional indicator split — flag in report but not an error.
KNOWN_INTENTIONAL = {
    ("athletics", "SCH_SSATHLETICS_IND"): (
        "Intentional split: measures single-sex athletics only (2017-18, 2020-21). "
        "SCH_ATHLETICS_IND (2021-22) covers all interscholastic athletics. "
        "Both columns coexist in staging with NULLs for non-applicable years."
    ),
    ("athletics", "SCH_ATHLETICS_IND"): (
        "Intentional split: measures all interscholastic athletics (2021-22). "
        "SCH_SSATHLETICS_IND (2017-18, 2020-21) covers single-sex only. "
        "Both columns coexist in staging with NULLs for non-applicable years."
    ),
}


def slugify(stem: str) -> str:
    s = stem.lower().strip()
    s = re.sub(r"[^a-z0-9\s]", "", s)
    s = re.sub(r"\s+", "_", s)
    return s.strip("_")


def get_canonical(stem: str, vintage: str) -> str:
    return FILE_OVERRIDES.get(vintage, {}).get(stem, slugify(stem))


def read_header(path: Path) -> list[str]:
    """Read only the first row of a CSV file."""
    with open(path, newline="", encoding="utf-8-sig") as f:
        return list(next(csv.reader(f)))


def build_inventory() -> dict[str, dict[str, set[str]]]:
    """
    Returns: {canonical_name: {vintage: set(column_names)}}
    """
    inventory: dict[str, dict[str, set[str]]] = defaultdict(lambda: defaultdict(set))

    for vintage in VINTAGES:
        sch_dir = SCH_DIRS[vintage]
        if not sch_dir.exists():
            raise FileNotFoundError(
                f"SCH directory not found for {vintage}: {sch_dir}\n"
                "Have you run download.py for this vintage?"
            )
        for csv_path in sorted(sch_dir.glob("*.csv")):
            canonical = get_canonical(csv_path.stem, vintage)
            headers = read_header(csv_path)
            inventory[canonical][vintage].update(headers)

    return inventory


def write_full_inventory(inventory: dict, out_path: Path) -> None:
    rows = []
    for canonical, vintages_data in sorted(inventory.items()):
        for vintage in VINTAGES:
            for col in sorted(vintages_data.get(vintage, [])):
                rows.append((canonical, col, vintage))

    with open(out_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["canonical_file", "column_name", "vintage"])
        writer.writerows(rows)

    print(f"  Wrote {len(rows)} rows → {out_path.name}")


def write_diff_report(inventory: dict, out_path: Path) -> int:
    """
    One row per (canonical_file, column) where the column is missing from
    at least one vintage. Returns total discrepancy row count.
    """
    rows = []
    for canonical, vintages_data in sorted(inventory.items()):
        all_cols = set()
        for cols in vintages_data.values():
            all_cols.update(cols)

        for col in sorted(all_cols):
            in_1718 = col in vintages_data.get("2017-18", set())
            in_2021 = col in vintages_data.get("2020-21", set())
            in_2122 = col in vintages_data.get("2021-22", set())

            if in_1718 and in_2021 and in_2122:
                continue  # present in all three — skip

            note = KNOWN_INTENTIONAL.get((canonical, col), "")
            rows.append((canonical, col, in_1718, in_2021, in_2122, note))

    with open(out_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "canonical_file", "column_name",
            "in_2017_18", "in_2020_21", "in_2021_22",
            "note",
        ])
        writer.writerows(rows)

    print(f"  Wrote {len(rows)} rows → {out_path.name}")
    return len(rows)


def write_summary(inventory: dict, total_discrepancies: int, out_path: Path) -> None:
    lines = []

    # File counts per vintage
    lines.append("=" * 60)
    lines.append("FILE COUNTS PER VINTAGE")
    lines.append("=" * 60)
    for vintage in VINTAGES:
        count = sum(1 for v in inventory.values() if vintage in v)
        lines.append(f"  {vintage}: {count} files")

    # All canonical file names across all vintages
    all_canonicals = set(inventory.keys())

    # Missing files
    lines.append("")
    lines.append("=" * 60)
    lines.append("FILES MISSING FROM ONE OR MORE VINTAGES")
    lines.append("=" * 60)
    missing_any = False
    for canonical in sorted(all_canonicals):
        present_in = [v for v in VINTAGES if v in inventory[canonical]]
        missing_from = [v for v in VINTAGES if v not in inventory[canonical]]
        if missing_from:
            missing_any = True
            lines.append(f"  {canonical}")
            lines.append(f"    Present in:  {', '.join(present_in)}")
            lines.append(f"    Missing from: {', '.join(missing_from)}")
    if not missing_any:
        lines.append("  None — all files present in all three vintages.")

    # Total discrepancies
    lines.append("")
    lines.append("=" * 60)
    lines.append("COLUMN DISCREPANCY SUMMARY")
    lines.append("=" * 60)
    lines.append(f"  Total discrepant columns across all files: {total_discrepancies}")

    # Per-file discrepancy counts
    per_file: dict[str, int] = defaultdict(int)
    for canonical, vintages_data in inventory.items():
        all_cols: set[str] = set()
        for cols in vintages_data.values():
            all_cols.update(cols)
        for col in all_cols:
            in_all = all(col in vintages_data.get(v, set()) for v in VINTAGES
                         if v in inventory[canonical])
            if not in_all:
                per_file[canonical] += 1

    lines.append("")
    lines.append("  Top 5 files by column discrepancy count:")
    top5 = sorted(per_file.items(), key=lambda x: x[1], reverse=True)[:5]
    for canonical, count in top5:
        lines.append(f"    {canonical}: {count} discrepant columns")

    # Full per-file discrepancy list
    lines.append("")
    lines.append("  All files with discrepancies:")
    for canonical, count in sorted(per_file.items(), key=lambda x: x[1], reverse=True):
        lines.append(f"    {canonical}: {count}")

    out_path.write_text("\n".join(lines) + "\n")
    print(f"  Wrote → {out_path.name}")


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    print("Building column inventory...")
    inventory = build_inventory()
    print(f"  Found {len(inventory)} canonical files across all vintages.")

    print("Writing outputs...")
    write_full_inventory(inventory, OUT_DIR / "full_column_inventory.csv")
    total_disc = write_diff_report(inventory, OUT_DIR / "column_diff_report.csv")
    write_summary(inventory, total_disc, OUT_DIR / "summary.txt")
    print("Done.")


if __name__ == "__main__":
    main()
