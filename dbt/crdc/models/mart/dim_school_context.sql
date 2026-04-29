{{ config(materialized='table') }}

with schools as (
    select combokey, survey_year
    from {{ ref('dim_school') }}
),

crosswalk as (
    select combokey, ncessch, survey_year
    from staging.crdc_ccd_crosswalk
),

ccd as (
    select * from staging.ccd_schools_2021
    union all
    select * from staging.ccd_schools_2022
),

joined as (
    select
        s.combokey,
        s.survey_year,
        xw.ncessch,
        c.locale,
        c.title_i_eligible,
        c.school_level,
        c.total_enrollment,
        c.frpl_count,
        c.pupil_teacher_ratio
    from schools s
    left join crosswalk xw
        on s.combokey = xw.combokey
        and s.survey_year = xw.survey_year
    left join ccd c
        on xw.ncessch = c.ncessch
        and s.survey_year = c.survey_year
)

select
    combokey,
    survey_year,
    ncessch,
    case left(locale, 1)
        when '1' then 'city'
        when '2' then 'suburb'
        when '3' then 'town'
        when '4' then 'rural'
    end as locale_type,
    locale as locale_raw,
    case title_i_eligible
        when '1-Yes' then true
        when '2-No' then false
        else null
    end as title_i_eligible,
    case school_level
        when 'Not Applicable' then null
        when 'Not Reported' then null
        else school_level
    end as school_level,
    total_enrollment,
    frpl_count,
    pupil_teacher_ratio
from joined