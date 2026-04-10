"""
Cross-vintage file and column normalization for the CRDC pipeline.

This module is the single authoritative source for all schema decisions
that span multiple vintages. When OCR releases a new vintage with renamed
files or columns, this file is updated — not the core pipeline logic.

Three responsibilities:
  1. Folder paths — locate SCH and LEA directories for each vintage
  2. File name canonicalization — map raw file names to stable table names
  3. Column normalization — rename columns to a consistent canonical form

Public API:
  get_sch_dir(vintage, data_dir)          → Path
  get_lea_dir(vintage, data_dir)          → Path
  get_canonical_name(stem, vintage)       → str
  normalize_column(col, canonical, vintage) → str
"""

import re
from pathlib import Path


# ── 1. Folder paths ────────────────────────────────────────────────────────────
#
# Paths are relative to data/raw/. Each vintage has a different internal
# folder structure — path normalization is a core responsibility of this module.

_VINTAGE_SCH_PATH: dict[str, Path] = {
    "2021-22": Path("2021-22") / "SCH",
    "2020-21": Path("2020-21") / "CRDC" / "School",
    "2017-18": (
        Path("2017-18")
        / "2017-18-crdc-data-corrected-publication 2"
        / "2017-18 Public-Use Files"
        / "Data"
        / "SCH"
        / "CRDC"
        / "CSV"
    ),
}

_VINTAGE_LEA_PATH: dict[str, Path] = {
    "2021-22": Path("2021-22") / "LEA",
    "2020-21": Path("2020-21") / "CRDC" / "LEA",
    "2017-18": (
        Path("2017-18")
        / "2017-18-crdc-data-corrected-publication 2"
        / "2017-18 Public-Use Files"
        / "Data"
        / "LEA"
        / "CRDC"
        / "CSV"
    ),
}


# ── 2. File name canonicalization ──────────────────────────────────────────────
#
# Maps raw file stems to canonical table names for files whose names differ
# across vintages. Files not listed here use the default slugification:
# lowercase the stem, replace spaces with underscores, strip punctuation.
#
# Canonical names are stable identifiers used as table names in the database.
# They do not change when OCR renames a file in a future vintage — only this
# dict is updated.

_FILE_OVERRIDES: dict[str, dict[str, str]] = {
    "2021-22": {
        "Interscholastic Athletics":    "athletics",
        "Single Sex Classes":           "single_sex_classes",
        "High School Equivalency Exam": "hs_equivalency",
    },
    "2020-21": {
        "Single sex Athletics":    "athletics",
        "Single sex Classes":      "single_sex_classes",
        "High School Equivalency": "hs_equivalency",
    },
    "2017-18": {
        "Single-sex Athletics":          "athletics",
        "Single-sex Classes":            "single_sex_classes",
        "High School Equivalency (GED)": "hs_equivalency",
    },
}


# ── 3. Column rename rules ─────────────────────────────────────────────────────

# 3a. Systematic rename: _LEP_ → _EL_
#
# OCR formally shifted terminology from "Limited English Proficient" (LEP)
# to "English Learner" (EL) in the 2021-22 collection. This affects 23 files
# in 2017-18 and 2020-21. No file in either vintage uses both conventions —
# the rename is safe to apply globally.
#
# Verified: no file in 2017-18 or 2020-21 contains both _LEP_ and _EL_ columns.
# 2021-22 files already use _EL_ and are unaffected.

_SYSTEMATIC_RENAMES: dict[str, dict[str, str]] = {
    "2017-18": {"_LEP_": "_EL_"},
    "2020-21": {"_LEP_": "_EL_"},
    "2021-22": {},
}

# 3b. Per-file exact renames keyed by (canonical_name, vintage)
#
# Each entry maps a raw column name to its canonical form for a specific
# file + vintage combination. These are applied before the systematic rename.
#
# ATHLETICS NOTE — intentional indicator split:
#   SCH_SSATHLETICS_IND (2017-18, 2020-21) measured single-sex athletics only.
#   SCH_ATHLETICS_IND (2021-22) measures all interscholastic athletics.
#   These are different questions and are intentionally NOT mapped to each other.
#   Both columns coexist in the staging table; each vintage gets NULLs for the
#   column it did not collect. SCH_SSATHLETICS_IND is absent from this rename
#   map by design.
#
#   The six follow-up questions (sports, teams, participants by sex) measured
#   the same things across all three vintages — only the column prefix changed
#   from SCH_SS* to SCH_* when OCR dropped "single-sex" from the file name.
#   These are safe renames.

_PER_FILE_RENAMES: dict[tuple[str, str], dict[str, str]] = {

    ("athletics", "2017-18"): {
        "SCH_SSSPORTS_M": "SCH_SPORTS_M",
        "SCH_SSSPORTS_F": "SCH_SPORTS_F",
        "TOT_SSSPORTS":   "TOT_SPORTS",
        "SCH_SSTEAMS_M":  "SCH_TEAMS_M",
        "SCH_SSTEAMS_F":  "SCH_TEAMS_F",
        "TOT_SSTEAMS":    "TOT_TEAMS",
        "SCH_SSPART_M":   "SCH_PART_M",
        "SCH_SSPART_F":   "SCH_PART_F",
        "TOT_SSPART":     "TOT_PART",
        # SCH_SSATHLETICS_IND intentionally excluded — see note above.
    },

    ("athletics", "2020-21"): {
        "SCH_SSSPORTS_M": "SCH_SPORTS_M",
        "SCH_SSSPORTS_F": "SCH_SPORTS_F",
        "TOT_SSSPORTS":   "TOT_SPORTS",
        "SCH_SSTEAMS_M":  "SCH_TEAMS_M",
        "SCH_SSTEAMS_F":  "SCH_TEAMS_F",
        "TOT_SSTEAMS":    "TOT_TEAMS",
        "SCH_SSPART_M":   "SCH_PART_M",
        "SCH_SSPART_F":   "SCH_PART_F",
        "TOT_SSPART":     "TOT_PART",
        # SCH_SSATHLETICS_IND intentionally excluded — see note above.
    },

    # dual_enrollment: total column naming made consistent with disaggregated
    # columns (which already used DUALENR) in 2021-22.
    ("dual_enrollment", "2017-18"): {
        "TOT_DUAL_F": "TOT_DUALENR_F",
        "TOT_DUAL_M": "TOT_DUALENR_M",
    },
    ("dual_enrollment", "2020-21"): {
        "TOT_DUAL_F": "TOT_DUALENR_F",
        "TOT_DUAL_M": "TOT_DUALENR_M",
    },

    # corporal_punishment: indicator column name shortened in 2021-22.
    # Verified same question across all three vintages.
    ("corporal_punishment", "2017-18"): {
        "SCH_CORPINSTANCES_IND": "SCH_CORP_IND",
    },
    ("corporal_punishment", "2020-21"): {
        "SCH_CORPINSTANCES_IND": "SCH_CORP_IND",
    },

    # covid_directional_indicators: OCR replaced "virtual" with "remote"
    # terminology between 2020-21 and 2021-22. The survey question and
    # response options are structurally identical; only the label changed.
    # This file does not exist in 2017-18 — the rename is 2020-21 only.
    ("covid_directional_indicators", "2020-21"): {
        "SCH_DIND_VIRTUALTYPE": "SCH_DIND_REMOTETYPE",
    },
}


# ── Internal helpers ───────────────────────────────────────────────────────────

def _slugify(stem: str) -> str:
    """Default canonical name: lowercase, spaces → underscores, strip punctuation."""
    s = stem.lower().strip()
    s = re.sub(r"[^a-z0-9\s]", "", s)
    s = re.sub(r"\s+", "_", s)
    return s.strip("_")


# ── Public API ─────────────────────────────────────────────────────────────────

def get_sch_dir(vintage: str, data_dir: Path) -> Path:
    """Return the Path to the SCH directory for the given vintage."""
    if vintage not in _VINTAGE_SCH_PATH:
        raise ValueError(
            f"Unknown vintage: {vintage!r}. Known vintages: {list(_VINTAGE_SCH_PATH)}"
        )
    return data_dir / _VINTAGE_SCH_PATH[vintage]


def get_lea_dir(vintage: str, data_dir: Path) -> Path:
    """Return the Path to the LEA directory for the given vintage."""
    if vintage not in _VINTAGE_LEA_PATH:
        raise ValueError(
            f"Unknown vintage: {vintage!r}. Known vintages: {list(_VINTAGE_LEA_PATH)}"
        )
    return data_dir / _VINTAGE_LEA_PATH[vintage]


def get_canonical_name(stem: str, vintage: str) -> str:
    """
    Return the canonical table name for a given file stem and vintage.

    Checks the override dict first; falls back to default slugification.
    """
    if vintage not in _VINTAGE_SCH_PATH:
        raise ValueError(
            f"Unknown vintage: {vintage!r}. Known vintages: {list(_VINTAGE_SCH_PATH)}"
        )
    return _FILE_OVERRIDES.get(vintage, {}).get(stem, _slugify(stem))


def normalize_column(col: str, canonical_name: str, vintage: str) -> str:
    """
    Return the canonical column name for a given raw column, file, and vintage.

    Applies transformations in this order:
      1. Per-file exact renames (higher specificity, applied first)
      2. Systematic pattern renames (e.g. _LEP_ → _EL_)

    If no rename applies, the column name is returned unchanged.
    """
    if vintage not in _VINTAGE_SCH_PATH:
        raise ValueError(
            f"Unknown vintage: {vintage!r}. Known vintages: {list(_VINTAGE_SCH_PATH)}"
        )

    # 1. Per-file exact rename
    exact = _PER_FILE_RENAMES.get((canonical_name, vintage), {})
    if col in exact:
        return exact[col]

    # 2. Systematic pattern rename
    for old_pat, new_pat in _SYSTEMATIC_RENAMES.get(vintage, {}).items():
        if old_pat in col:
            col = col.replace(old_pat, new_pat)

    return col
