{{ config(materialized='table') }}

with internet as (
    select * from {{ ref('fct_internet') }}
),

school as (
    select
        combokey,
        survey_year,
        school_name,
        grade_span,
        status_sped,
        status_alt,
        status_charter,
        status_magnet,
        jj
    from {{ ref('dim_school') }}
),

context as (
    select
        combokey,
        survey_year,
        locale_type,
        title_i_eligible,
        school_level,
        frpl_count,
        total_enrollment as ccd_total_enrollment
    from {{ ref('dim_school_context') }}
),

enrollment_pivot as (
    select
        combokey,
        survey_year,
        sum(case when subgroup = 'total'           then enrollment_count end) as total_enrollment,
        sum(case when subgroup = 'black'           then enrollment_count end) as enr_black,
        sum(case when subgroup = 'hispanic'        then enrollment_count end) as enr_hispanic,
        sum(case when subgroup = 'white'           then enrollment_count end) as enr_white,
        sum(case when subgroup = 'asian'           then enrollment_count end) as enr_asian,
        sum(case when subgroup = 'american_indian' then enrollment_count end) as enr_american_indian,
        sum(case when subgroup = 'idea'            then enrollment_count end) as enr_idea,
        sum(case when subgroup = 'english_learner' then enrollment_count end) as enr_el,
        sum(case when subgroup = 'section_504'     then enrollment_count end) as enr_section_504
    from {{ ref('fct_enrollment_k12') }}
    group by combokey, survey_year
)

select
    i.combokey,
    i.lea_id,
    i.survey_year,
    s.school_name,

    -- internet indicators
    i.sch_internet_fiber,
    i.sch_internet_wifi,
    i.sch_internet_schdev,
    i.sch_internet_studdev,
    i.sch_internet_wifiendev,

    -- school characteristics
    s.grade_span,
    s.status_sped,
    s.status_alt,
    s.status_charter,
    s.status_magnet,
    s.jj,

    -- CCD context, including FRPL percentage
    ctx.locale_type,
    ctx.school_level,
    case
        when ctx.title_i_eligible = true  then 'Yes'
        when ctx.title_i_eligible = false then 'No'
    end as title_i_eligible,
    ctx.ccd_total_enrollment,
    round(100.0 * ctx.frpl_count / nullif(ctx.ccd_total_enrollment, 0), 1) as frpl_pct,

    -- enrollment percentages
    e.total_enrollment,
    round(100.0 * e.enr_black           / nullif(e.total_enrollment, 0), 1) as pct_black,
    round(100.0 * e.enr_hispanic        / nullif(e.total_enrollment, 0), 1) as pct_hispanic,
    round(100.0 * e.enr_white           / nullif(e.total_enrollment, 0), 1) as pct_white,
    round(100.0 * e.enr_asian           / nullif(e.total_enrollment, 0), 1) as pct_asian,
    round(100.0 * e.enr_american_indian / nullif(e.total_enrollment, 0), 1) as pct_american_indian,
    round(100.0 * e.enr_idea            / nullif(e.total_enrollment, 0), 1) as pct_idea,
    round(100.0 * e.enr_el              / nullif(e.total_enrollment, 0), 1) as pct_el

from internet i
left join school s
    on i.combokey = s.combokey and i.survey_year = s.survey_year
left join context ctx
    on i.combokey = ctx.combokey and i.survey_year = ctx.survey_year
left join enrollment_pivot e
    on i.combokey = e.combokey and i.survey_year = e.survey_year