{{ config(materialized='table') }}

with src_2021 as (
    select
        combokey,
        leaid as lea_id,
        survey_year,
        sch_internet_fiber,
        sch_internet_wifi,
        sch_internet_schdev,
        sch_internet_studdev,
        sch_internet_wifiendev
    from staging.internet_access_and_devices_2021
),

src_2022 as (
    select
        combokey,
        leaid as lea_id,
        survey_year,
        sch_internet_fiber,
        sch_internet_wifi,
        sch_internet_schdev,
        sch_internet_studdev,
        sch_internet_wifiendev
    from staging.internet_access_and_devices_2022
)

select * from src_2021
union all
select * from src_2022