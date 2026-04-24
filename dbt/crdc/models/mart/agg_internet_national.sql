{{ config(materialized='table') }}

select
    survey_year,
    count(*) as total_schools,
    count(case when sch_internet_fiber = 'Yes' then 1 end)
        as schools_with_fiber,
    round(100.0 * count(case when sch_internet_fiber = 'Yes' then 1 end)
        / nullif(count(sch_internet_fiber),0), 1) as pct_fiber,
    count(case when sch_internet_wifi = 'Yes' then 1 end)
        as schools_with_wifi,
    round(100.0 * count(case when sch_internet_wifi = 'Yes' then 1 end)
        / nullif(count(sch_internet_wifi),0), 1) as pct_wifi,
    count(case when sch_internet_schdev = 'Yes' then 1 end)
        as schools_with_schdev,
    round(100.0 * count(case when sch_internet_schdev = 'Yes' then 1 end)
        / nullif(count(sch_internet_schdev),0), 1) as pct_schdev,
    count(case when sch_internet_studdev = 'Yes' then 1 end)
        as schools_with_studdev,
    round(100.0 * count(case when sch_internet_studdev = 'Yes' then 1 end)
        / nullif(count(sch_internet_studdev),0), 1) as pct_studdev,
    round(avg(sch_internet_wifiendev),1)
        as avg_wifiendev
from {{ ref('fct_internet') }}
group by survey_year
order by survey_year