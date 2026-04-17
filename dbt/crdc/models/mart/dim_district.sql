{{ config(materialized='table') }}

with src_2018 as (
    select
        leaid as lea_id,
        survey_year,
        lea_name,
        lea_state as state,
        lea_state_name as state_name,
        lea_zip::text as zip_code,
        lea_enr as total_enrollment,
        lea_enr_nonleafac as total_enrollment_nonlea,
        lea_schools as school_count,
        lea_ps_ind as ps_ind,
        lea_crcoord_sex_ind as crcoord_sex_ind,
        lea_crcoord_rac_ind as crcoord_rac_ind,
        lea_crcoord_dis_ind as crcoord_dis_ind,
        lea_desegplan as desegplan_ind,
        lea_hbpolicy_ind as hbpolicy_ind,
        lea_hbpolicyurl_ind as hbpolicyurl_ind
    from staging.lea_characteristics_2018
),

src_2021 as (
    select
        leaid as lea_id,
        survey_year,
        lea_name,
        lea_state as state,
        lea_state_name as state_name,
        lea_zip::text as zip_code,
        lea_enr as total_enrollment,
        lea_enr_nonleafac as total_enrollment_nonlea,
        lea_schools as school_count,
        lea_ps_ind as ps_ind,
        lea_crcoord_sex_ind as crcoord_sex_ind,
        lea_crcoord_rac_ind as crcoord_rac_ind,
        lea_crcoord_dis_ind as crcoord_dis_ind,
        lea_desegplan as desegplan_ind,
        lea_hbpolicy_ind as hbpolicy_ind,
        case
            when lea_hbpolicy_url is not null then 'Yes'
            else 'No'
        end as hbpolicyurl_ind
    from staging.lea_characteristics_2021
),

src_2022 as (
    select
        leaid as lea_id,
        survey_year,
        lea_name,
        lea_state as state,
        lea_state_name as state_name,
        lea_zip::text as zip_code,
        lea_enr as total_enrollment,
        lea_enr_nonleafac as total_enrollment_nonlea,
        lea_schools as school_count,
        lea_ps_ind as ps_ind,
        lea_crcoord_sex_ind as crcoord_sex_ind,
        lea_crcoord_rac_ind as crcoord_rac_ind,
        lea_crcoord_dis_ind as crcoord_dis_ind,
        lea_desegplan as desegplan_ind,
        lea_hbpolicy_ind  as hbpolicy_ind,
        lea_hbpolicyurl_ind as hbpolicyurl_ind
    from staging.lea_characteristics_2022
)

select * from src_2018
union all
select * from src_2021
union all
select * from src_2022