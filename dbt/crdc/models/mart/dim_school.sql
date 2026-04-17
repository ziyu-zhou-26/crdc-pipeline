{{ config(materialized='table') }}

with src_2018 as (
    select
        combokey,
        leaid as lea_id,
        schid as school_id,
        sch_name as school_name,
        survey_year,
        jj,
        NULL::text as virt_ind,
        case
            when sch_grade_ps = 'Yes' then 'PK'
            when sch_grade_kg = 'Yes' then 'KG'
            when sch_grade_g01 = 'Yes' then '01'
            when sch_grade_g02 = 'Yes' then '02'
            when sch_grade_g03 = 'Yes' then '03'
            when sch_grade_g04 = 'Yes' then '04'
            when sch_grade_g05 = 'Yes' then '05'
            when sch_grade_g06 = 'Yes' then '06'
            when sch_grade_g07 = 'Yes' then '07'
            when sch_grade_g08 = 'Yes' then '08'
            when sch_grade_g09 = 'Yes' then '09'
            when sch_grade_g10 = 'Yes' then '10'
            when sch_grade_g11 = 'Yes' then '11'
            when sch_grade_g12 = 'Yes' then '12'
        end as grade_low,
        case
            when sch_grade_g12 = 'Yes' then '12'
            when sch_grade_g11 = 'Yes' then '11'
            when sch_grade_g10 = 'Yes' then '10'
            when sch_grade_g09 = 'Yes' then '09'
            when sch_grade_g08 = 'Yes' then '08'
            when sch_grade_g07 = 'Yes' then '07'
            when sch_grade_g06 = 'Yes' then '06'
            when sch_grade_g05 = 'Yes' then '05'
            when sch_grade_g04 = 'Yes' then '04'
            when sch_grade_g03 = 'Yes' then '03'
            when sch_grade_g02 = 'Yes' then '02'
            when sch_grade_g01 = 'Yes' then '01'
            when sch_grade_kg = 'Yes' then 'KG'
            when sch_grade_ps = 'Yes' then 'PK'
        end as grade_high,
        sch_grade_ug as grade_ug,
        sch_ugdetail_es as ugdetail_es,
        sch_ugdetail_ms as ugdetail_ms,
        sch_ugdetail_hs as ugdetail_hs,
        sch_status_sped as status_sped,
        sch_status_magnet as status_magnet,
        sch_magnetdetail as magnet_detail,
        sch_status_charter as status_charter,
        sch_status_alt as status_alt,
        sch_altfocus as alt_focus
    from staging.school_characteristics_2018
),

src_2021 as (
    select
        combokey,
        leaid as lea_id,
        schid as school_id,
        sch_name as school_name,
        survey_year,
        jj,
        NULL::text as virt_ind,
        case
            when sch_grade_ps = 'Yes' then 'PK'
            when sch_grade_kg = 'Yes' then 'KG'
            when sch_grade_g01 = 'Yes' then '01'
            when sch_grade_g02 = 'Yes' then '02'
            when sch_grade_g03 = 'Yes' then '03'
            when sch_grade_g04 = 'Yes' then '04'
            when sch_grade_g05 = 'Yes' then '05'
            when sch_grade_g06 = 'Yes' then '06'
            when sch_grade_g07 = 'Yes' then '07'
            when sch_grade_g08 = 'Yes' then '08'
            when sch_grade_g09 = 'Yes' then '09'
            when sch_grade_g10 = 'Yes' then '10'
            when sch_grade_g11 = 'Yes' then '11'
            when sch_grade_g12 = 'Yes' then '12'
        end as grade_low,
        case
            when sch_grade_g12 = 'Yes' then '12'
            when sch_grade_g11 = 'Yes' then '11'
            when sch_grade_g10 = 'Yes' then '10'
            when sch_grade_g09 = 'Yes' then '09'
            when sch_grade_g08 = 'Yes' then '08'
            when sch_grade_g07 = 'Yes' then '07'
            when sch_grade_g06 = 'Yes' then '06'
            when sch_grade_g05 = 'Yes' then '05'
            when sch_grade_g04 = 'Yes' then '04'
            when sch_grade_g03 = 'Yes' then '03'
            when sch_grade_g02 = 'Yes' then '02'
            when sch_grade_g01 = 'Yes' then '01'
            when sch_grade_kg = 'Yes' then 'KG'
            when sch_grade_ps = 'Yes' then 'PK'
        end as grade_high,
        sch_grade_ug as grade_ug,
        sch_ugdetail_es as ugdetail_es,
        sch_ugdetail_ms as ugdetail_ms,
        sch_ugdetail_hs as ugdetail_hs,
        sch_status_sped as status_sped,
        sch_status_magnet as status_magnet,
        sch_magnetdetail as magnet_detail,
        sch_status_charter as status_charter,
        sch_status_alt as status_alt,
        sch_altfocus as alt_focus
    from staging.school_characteristics_2021
),

src_2022 as (
    select
        combokey,
        leaid as lea_id,
        schid as school_id,
        sch_name as school_name,
        survey_year,
        jj,
        sch_virt_ind as virt_ind,
        case
            when sch_grade_ps = 'Yes' then 'PK'
            when sch_grade_kg = 'Yes' then 'KG'
            when sch_grade_g01 = 'Yes' then '01'
            when sch_grade_g02 = 'Yes' then '02'
            when sch_grade_g03 = 'Yes' then '03'
            when sch_grade_g04 = 'Yes' then '04'
            when sch_grade_g05 = 'Yes' then '05'
            when sch_grade_g06 = 'Yes' then '06'
            when sch_grade_g07 = 'Yes' then '07'
            when sch_grade_g08 = 'Yes' then '08'
            when sch_grade_g09 = 'Yes' then '09'
            when sch_grade_g10 = 'Yes' then '10'
            when sch_grade_g11 = 'Yes' then '11'
            when sch_grade_g12 = 'Yes' then '12'
        end as grade_low,
        case
            when sch_grade_g12 = 'Yes' then '12'
            when sch_grade_g11 = 'Yes' then '11'
            when sch_grade_g10 = 'Yes' then '10'
            when sch_grade_g09 = 'Yes' then '09'
            when sch_grade_g08 = 'Yes' then '08'
            when sch_grade_g07 = 'Yes' then '07'
            when sch_grade_g06 = 'Yes' then '06'
            when sch_grade_g05 = 'Yes' then '05'
            when sch_grade_g04 = 'Yes' then '04'
            when sch_grade_g03 = 'Yes' then '03'
            when sch_grade_g02 = 'Yes' then '02'
            when sch_grade_g01 = 'Yes' then '01'
            when sch_grade_kg = 'Yes' then 'KG'
            when sch_grade_ps = 'Yes' then 'PK'
        end as grade_high,
        sch_grade_ug as grade_ug,
        sch_ugdetail_es as ugdetail_es,
        sch_ugdetail_ms as ugdetail_ms,
        sch_ugdetail_hs as ugdetail_hs,
        sch_status_sped as status_sped,
        sch_status_magnet as status_magnet,
        sch_magnetdetail as magnet_detail,
        sch_status_charter as status_charter,
        sch_status_alt as status_alt,
        sch_altfocus as alt_focus
    from staging.school_characteristics_2022
),

unioned as (
    select * from src_2018
    union all
    select * from src_2021
    union all
    select * from src_2022
)

select
    combokey,
    lea_id,
    school_id,
    school_name,
    survey_year,
    jj,
    virt_ind,
    case
        when grade_low is not null then grade_low || '-' || grade_high
        when grade_ug = 'Yes' then 'Ungraded'
    end as grade_span,
    status_sped,
    status_magnet,
    magnet_detail,
    status_charter,
    status_alt,
    alt_focus
from unioned