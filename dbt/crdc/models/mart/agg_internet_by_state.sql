{{ config(materialized='table') }}

with internet as (
    select
        i.survey_year,
        i.sch_internet_fiber,
        i.sch_internet_wifi,
        i.sch_internet_schdev,
        i.sch_internet_studdev,
        i.sch_internet_wifiendev,
        d.state,
        d.state_name
    from {{ ref('fct_internet') }} i
    join {{ ref('dim_district') }} d
    on i.lea_id = d.lea_id
    and i.survey_year = d.survey_year
)

select
    state,
    state_name,
    survey_year,
    count(*)
        as total_schools,
    sum(case when sch_internet_fiber = 'Yes' then 1 end)
        as schools_with_fiber,
    round(100.0 * count(case when sch_internet_fiber = 'Yes' then 1 end)
        / nullif(count(sch_internet_fiber),0), 1) as pct_fiber,
    sum(case when sch_internet_wifi = 'Yes' then 1 end)
        as schools_with_wifi,
    round(100.0 * count(case when sch_internet_wifi = 'Yes' then 1 end)
        / nullif(count(sch_internet_wifi),0), 1) as pct_wifi,
    sum(case when sch_internet_schdev = 'Yes' then 1 end)
        as schools_with_schdev,
    round(100.0 * count(case when sch_internet_schdev = 'Yes' then 1 end)
        / nullif(count(sch_internet_schdev),0), 1) as pct_schdev,
    sum(case when sch_internet_studdev = 'Yes' then 1 end)
        as schools_with_studdev,
    round(100.0 * count(case when sch_internet_studdev = 'Yes' then 1 end)
        / nullif(count(sch_internet_studdev),0), 1) as pct_studdev,
    round(avg(sch_internet_wifiendev),1)
        as avg_wifiendev
from internet
group by state, state_name, survey_year
order by state, survey_year