"""
Tests for ingestion/utils/schema_map.py

Covers all four public functions:
  - get_sch_dir / get_lea_dir   (folder path resolution)
  - get_canonical_name          (file name normalization)
  - normalize_column            (column rename logic)
"""

import pytest
from pathlib import Path

from ingestion.utils.schema_map import (
    get_sch_dir,
    get_lea_dir,
    get_canonical_name,
    normalize_column,
)

DATA_DIR = Path(__file__).resolve().parents[1] / "data" / "raw"
KNOWN_VINTAGES = ["2021-22", "2020-21", "2017-18"]


# ── get_sch_dir / get_lea_dir ──────────────────────────────────────────────────

class TestGetDirs:

    def test_sch_dirs_exist(self):
        """All three vintage SCH directories resolve to real paths on disk."""
        for vintage in KNOWN_VINTAGES:
            p = get_sch_dir(vintage, DATA_DIR)
            assert p.exists(), f"SCH dir missing for {vintage}: {p}"
            assert p.is_dir()

    def test_lea_dirs_exist(self):
        """All three vintage LEA directories resolve to real paths on disk."""
        for vintage in KNOWN_VINTAGES:
            p = get_lea_dir(vintage, DATA_DIR)
            assert p.exists(), f"LEA dir missing for {vintage}: {p}"
            assert p.is_dir()

    def test_sch_dir_contains_csvs(self):
        """Each SCH directory contains at least one CSV file."""
        for vintage in KNOWN_VINTAGES:
            csvs = list(get_sch_dir(vintage, DATA_DIR).glob("*.csv"))
            assert len(csvs) > 0, f"No CSV files found in SCH dir for {vintage}"

    def test_unknown_vintage_raises_sch(self):
        with pytest.raises(ValueError, match="Unknown vintage"):
            get_sch_dir("2019-20", DATA_DIR)

    def test_unknown_vintage_raises_lea(self):
        with pytest.raises(ValueError, match="Unknown vintage"):
            get_lea_dir("2019-20", DATA_DIR)


# ── get_canonical_name ─────────────────────────────────────────────────────────

class TestGetCanonicalName:

    # Known overrides — athletics
    @pytest.mark.parametrize("stem,vintage", [
        ("Interscholastic Athletics", "2021-22"),
        ("Single sex Athletics",      "2020-21"),
        ("Single-sex Athletics",      "2017-18"),
    ])
    def test_athletics_override(self, stem, vintage):
        assert get_canonical_name(stem, vintage) == "athletics"

    # Known overrides — single sex classes
    @pytest.mark.parametrize("stem,vintage", [
        ("Single Sex Classes",  "2021-22"),
        ("Single sex Classes",  "2020-21"),
        ("Single-sex Classes",  "2017-18"),
    ])
    def test_single_sex_classes_override(self, stem, vintage):
        assert get_canonical_name(stem, vintage) == "single_sex_classes"

    # Known overrides — hs equivalency
    @pytest.mark.parametrize("stem,vintage", [
        ("High School Equivalency Exam",  "2021-22"),
        ("High School Equivalency",       "2020-21"),
        ("High School Equivalency (GED)", "2017-18"),
    ])
    def test_hs_equivalency_override(self, stem, vintage):
        assert get_canonical_name(stem, vintage) == "hs_equivalency"

    # Default slugification — no override needed
    @pytest.mark.parametrize("stem,expected", [
        ("Enrollment",            "enrollment"),
        ("Advanced Placement",    "advanced_placement"),
        ("Corporal Punishment",   "corporal_punishment"),
        ("Harassment and Bullying", "harassment_and_bullying"),
        ("SAT and ACT",           "sat_and_act"),
        ("School Characteristics","school_characteristics"),
    ])
    def test_default_slugification(self, stem, expected):
        assert get_canonical_name(stem, "2021-22") == expected

    def test_unknown_vintage_raises(self):
        with pytest.raises(ValueError, match="Unknown vintage"):
            get_canonical_name("Enrollment", "2019-20")


# ── normalize_column ───────────────────────────────────────────────────────────

class TestNormalizeColumn:

    # Systematic LEP → EL rename
    @pytest.mark.parametrize("vintage", ["2017-18", "2020-21"])
    def test_lep_to_el_renamed(self, vintage):
        assert normalize_column("SCH_ENR_LEP_F", "enrollment", vintage) == "SCH_ENR_EL_F"
        assert normalize_column("SCH_ENR_LEP_M", "enrollment", vintage) == "SCH_ENR_EL_M"
        assert normalize_column("SCH_GTENR_LEP_F", "gifted_and_talented", vintage) == "SCH_GTENR_EL_F"

    def test_lep_not_renamed_in_2122(self):
        """2021-22 files already use _EL_ — LEP columns should not be touched."""
        assert normalize_column("SCH_ENR_LEP_F", "enrollment", "2021-22") == "SCH_ENR_LEP_F"

    def test_el_columns_unchanged(self):
        """Columns already using _EL_ naming should pass through unchanged."""
        assert normalize_column("SCH_ENR_EL_F", "enrollment", "2021-22") == "SCH_ENR_EL_F"
        assert normalize_column("SCH_ENR_EL_F", "enrollment", "2017-18") == "SCH_ENR_EL_F"

    # Athletics SS prefix renames
    @pytest.mark.parametrize("vintage", ["2017-18", "2020-21"])
    def test_athletics_ss_renames(self, vintage):
        assert normalize_column("SCH_SSSPORTS_M", "athletics", vintage) == "SCH_SPORTS_M"
        assert normalize_column("SCH_SSSPORTS_F", "athletics", vintage) == "SCH_SPORTS_F"
        assert normalize_column("TOT_SSSPORTS",   "athletics", vintage) == "TOT_SPORTS"
        assert normalize_column("SCH_SSTEAMS_M",  "athletics", vintage) == "SCH_TEAMS_M"
        assert normalize_column("SCH_SSTEAMS_F",  "athletics", vintage) == "SCH_TEAMS_F"
        assert normalize_column("TOT_SSTEAMS",    "athletics", vintage) == "TOT_TEAMS"
        assert normalize_column("SCH_SSPART_M",   "athletics", vintage) == "SCH_PART_M"
        assert normalize_column("SCH_SSPART_F",   "athletics", vintage) == "SCH_PART_F"
        assert normalize_column("TOT_SSPART",     "athletics", vintage) == "TOT_PART"

    @pytest.mark.parametrize("vintage", ["2017-18", "2020-21"])
    def test_athletics_indicator_not_renamed(self, vintage):
        """
        SCH_SSATHLETICS_IND must NOT be renamed. It measures single-sex athletics
        only, while SCH_ATHLETICS_IND (2021-22) measures all interscholastic
        athletics. These are different questions and coexist in the staging table.
        """
        assert normalize_column("SCH_SSATHLETICS_IND", "athletics", vintage) == "SCH_SSATHLETICS_IND"

    def test_athletics_2122_columns_unchanged(self):
        """2021-22 athletics columns are already canonical."""
        assert normalize_column("SCH_ATHLETICS_IND", "athletics", "2021-22") == "SCH_ATHLETICS_IND"
        assert normalize_column("SCH_SPORTS_M",      "athletics", "2021-22") == "SCH_SPORTS_M"

    # Dual enrollment total rename
    @pytest.mark.parametrize("vintage", ["2017-18", "2020-21"])
    def test_dual_enrollment_total_rename(self, vintage):
        assert normalize_column("TOT_DUAL_F", "dual_enrollment", vintage) == "TOT_DUALENR_F"
        assert normalize_column("TOT_DUAL_M", "dual_enrollment", vintage) == "TOT_DUALENR_M"

    def test_dual_enrollment_total_unchanged_in_2122(self):
        assert normalize_column("TOT_DUALENR_F", "dual_enrollment", "2021-22") == "TOT_DUALENR_F"

    # Corporal punishment indicator rename
    @pytest.mark.parametrize("vintage", ["2017-18", "2020-21"])
    def test_corporal_punishment_indicator_rename(self, vintage):
        assert normalize_column(
            "SCH_CORPINSTANCES_IND", "corporal_punishment", vintage
        ) == "SCH_CORP_IND"

    def test_corporal_punishment_indicator_unchanged_in_2122(self):
        assert normalize_column(
            "SCH_CORP_IND", "corporal_punishment", "2021-22"
        ) == "SCH_CORP_IND"

    # COVID directional indicators rename — 2020-21 only
    def test_covid_virtual_to_remote_rename(self):
        assert normalize_column(
            "SCH_DIND_VIRTUALTYPE", "covid_directional_indicators", "2020-21"
        ) == "SCH_DIND_REMOTETYPE"

    def test_covid_rename_does_not_apply_to_2122(self):
        """The COVID file does not exist in 2017-18; the rename is 2020-21 only."""
        assert normalize_column(
            "SCH_DIND_VIRTUALTYPE", "covid_directional_indicators", "2021-22"
        ) == "SCH_DIND_VIRTUALTYPE"

    # No-op cases
    def test_non_rename_column_passes_through(self):
        """Columns with no applicable rename are returned unchanged."""
        assert normalize_column("COMBOKEY",    "enrollment", "2017-18") == "COMBOKEY"
        assert normalize_column("SCH_ENR_HI_M","enrollment", "2021-22") == "SCH_ENR_HI_M"
        assert normalize_column("JJ",          "enrollment", "2020-21") == "JJ"

    def test_per_file_rename_does_not_bleed_to_other_files(self):
        """TOT_DUAL_F should only rename in dual_enrollment, not other files."""
        assert normalize_column("TOT_DUAL_F", "enrollment", "2017-18") == "TOT_DUAL_F"

    def test_unknown_vintage_raises(self):
        with pytest.raises(ValueError, match="Unknown vintage"):
            normalize_column("SCH_ENR_LEP_F", "enrollment", "2019-20")
