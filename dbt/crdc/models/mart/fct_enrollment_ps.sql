{{ config(materialized='table') }}

with src_2018 as (
    select
        combokey,
        leaid as lea_id,
        survey_year,
        sch_psenr_hi_m,
        sch_psenr_hi_f,
        sch_psenr_am_m,
        sch_psenr_am_f,
        sch_psenr_as_m,
        sch_psenr_as_f,
        sch_psenr_hp_m,
        sch_psenr_hp_f,
        sch_psenr_bl_m,
        sch_psenr_bl_f,
        sch_psenr_wh_m,
        sch_psenr_wh_f,
        sch_psenr_tr_m,
        sch_psenr_tr_f,
        tot_psenr_m,
        tot_psenr_f,
        sch_psenr_el_m,
        sch_psenr_el_f,
        sch_psenr_idea_m,
        sch_psenr_idea_f,
        NULL::bigint as sch_psenr_504_m,
        NULL::bigint as sch_psenr_504_f
    from staging.enrollment_2018
),

src_2021 as (
    select
        combokey,
        leaid as lea_id,
        survey_year,
        sch_psenr_hi_m,
        sch_psenr_hi_f,
        sch_psenr_am_m,
        sch_psenr_am_f,
        sch_psenr_as_m,
        sch_psenr_as_f,
        sch_psenr_hp_m,
        sch_psenr_hp_f,
        sch_psenr_bl_m,
        sch_psenr_bl_f,
        sch_psenr_wh_m,
        sch_psenr_wh_f,
        sch_psenr_tr_m,
        sch_psenr_tr_f,
        tot_psenr_m,
        tot_psenr_f,
        sch_psenr_el_m,
        sch_psenr_el_f,
        sch_psenr_idea_m,
        sch_psenr_idea_f,
        NULL::bigint as sch_psenr_504_m,
        NULL::bigint as sch_psenr_504_f
    from staging.enrollment_2021  
),

src_2022 as (
    select
        combokey,
        leaid as lea_id,
        survey_year,
        sch_psenr_hi_m,
        sch_psenr_hi_f,
        sch_psenr_am_m,
        sch_psenr_am_f,
        sch_psenr_as_m,
        sch_psenr_as_f,
        sch_psenr_hp_m,
        sch_psenr_hp_f,
        sch_psenr_bl_m,
        sch_psenr_bl_f,
        sch_psenr_wh_m,
        sch_psenr_wh_f,
        sch_psenr_tr_m,
        sch_psenr_tr_f,
        tot_psenr_m,
        tot_psenr_f,
        sch_psenr_el_m,
        sch_psenr_el_f,
        sch_psenr_idea_m,
        sch_psenr_idea_f,
        sch_psenr_504_m,
        sch_psenr_504_f
    from staging.enrollment_2022
),

unioned as (
    select * from src_2018
    union all
    select * from src_2021
    union all
    select * from src_2022
),

unpivoted as (
    select
        u.combokey,
        u.lea_id,
        u.survey_year,
        m.subgroup,
        m.sex,
        case m.col_name
            when 'sch_psenr_hi_m' then u.sch_psenr_hi_m
            when 'sch_psenr_hi_f' then u.sch_psenr_hi_f
            when 'sch_psenr_am_m' then u.sch_psenr_am_m
            when 'sch_psenr_am_f' then u.sch_psenr_am_f
            when 'sch_psenr_as_m' then u.sch_psenr_as_m
            when 'sch_psenr_as_f' then u.sch_psenr_as_f
            when 'sch_psenr_hp_m' then u.sch_psenr_hp_m
            when 'sch_psenr_hp_f' then u.sch_psenr_hp_f
            when 'sch_psenr_bl_m' then u.sch_psenr_bl_m
            when 'sch_psenr_bl_f' then u.sch_psenr_bl_f
            when 'sch_psenr_wh_m' then u.sch_psenr_wh_m
            when 'sch_psenr_wh_f' then u.sch_psenr_wh_f
            when 'sch_psenr_tr_m' then u.sch_psenr_tr_m
            when 'sch_psenr_tr_f' then u.sch_psenr_tr_f
            when 'tot_psenr_m' then u.tot_psenr_m
            when 'tot_psenr_f' then u.tot_psenr_f
            when 'sch_psenr_el_m' then u.sch_psenr_el_m
            when 'sch_psenr_el_f' then u.sch_psenr_el_f
            when 'sch_psenr_idea_m' then u.sch_psenr_idea_m
            when 'sch_psenr_idea_f' then u.sch_psenr_idea_f
            when 'sch_psenr_504_m' then u.sch_psenr_504_m
            when 'sch_psenr_504_f' then u.sch_psenr_504_f
        end as enrollment_count
    from unioned u
    cross join (
        values
            ('sch_psenr_hi_m', 'hispanic','male'),
            ('sch_psenr_hi_f', 'hispanic','female'),
            ('sch_psenr_am_m', 'american_indian', 'male'),
            ('sch_psenr_am_f', 'american_indian', 'female'),
            ('sch_psenr_as_m', 'asian', 'male'),                
            ('sch_psenr_as_f', 'asian', 'female'),
            ('sch_psenr_hp_m', 'hawaiian_pacific_islander', 'male'),                
            ('sch_psenr_hp_f', 'hawaiian_pacific_islander', 'female'),
            ('sch_psenr_bl_m', 'black', 'male'),                
            ('sch_psenr_bl_f', 'black', 'female'),          
            ('sch_psenr_wh_m', 'white', 'male'),                
            ('sch_psenr_wh_f', 'white', 'female'),         
            ('sch_psenr_tr_m', 'two_or_more_races', 'male'),
            ('sch_psenr_tr_f', 'two_or_more_races', 'female'),          
            ('tot_psenr_m', 'total', 'male'),
            ('tot_psenr_f', 'total', 'female'),         
            ('sch_psenr_el_m', 'english_learner', 'male'),
            ('sch_psenr_el_f', 'english_learner', 'female'),
            ('sch_psenr_idea_m', 'idea', 'male'),                
            ('sch_psenr_idea_f', 'idea', 'female'),
            ('sch_psenr_504_m', 'section_504', 'male'),                
            ('sch_psenr_504_f', 'section_504', 'female')
    ) as m(col_name, subgroup, sex)
)

select * from unpivoted