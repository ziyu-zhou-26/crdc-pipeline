{{ config(materialized='table') }}

with src_2018 as (
    select
        combokey,
        leaid as lea_id,
        survey_year,
        sch_enr_hi_m,
        sch_enr_hi_f,
        NULL::bigint as sch_enr_hi_x,
        sch_enr_am_m,
        sch_enr_am_f,
        NULL::bigint as sch_enr_am_x,
        sch_enr_as_m,
        sch_enr_as_f,
        NULL::bigint as sch_enr_as_x,
        sch_enr_hp_m,
        sch_enr_hp_f,
        NULL::bigint as sch_enr_hp_x,
        sch_enr_bl_m,
        sch_enr_bl_f,
        NULL::bigint as sch_enr_bl_x,
        sch_enr_wh_m,
        sch_enr_wh_f,
        NULL::bigint as sch_enr_wh_x,
        sch_enr_tr_m,
        sch_enr_tr_f,
        NULL::bigint as sch_enr_tr_x,
        tot_enr_m,
        tot_enr_f,
        NULL::bigint as tot_enr_x,       
        sch_enr_el_m,
        sch_enr_el_f,
        NULL::bigint as sch_enr_el_x,
        sch_enr_idea_m,
        sch_enr_idea_f,
        NULL::bigint as sch_enr_idea_x,
        sch_enr_504_m,
        sch_enr_504_f,
        NULL::bigint as sch_enr_504_x
    from staging.enrollment_2018
),

src_2021 as (
    select
        combokey,
        leaid as lea_id,
        survey_year,
        sch_enr_hi_m,
        sch_enr_hi_f,
        NULL::bigint as sch_enr_hi_x,
        sch_enr_am_m,
        sch_enr_am_f,
        NULL::bigint as sch_enr_am_x,
        sch_enr_as_m,
        sch_enr_as_f,
        NULL::bigint as sch_enr_as_x,
        sch_enr_hp_m,
        sch_enr_hp_f,
        NULL::bigint as sch_enr_hp_x,
        sch_enr_bl_m,
        sch_enr_bl_f,
        NULL::bigint as sch_enr_bl_x,
        sch_enr_wh_m,
        sch_enr_wh_f,
        NULL::bigint as sch_enr_wh_x,
        sch_enr_tr_m,
        sch_enr_tr_f,
        NULL::bigint as sch_enr_tr_x,
        tot_enr_m,
        tot_enr_f,
        NULL::bigint as tot_enr_x,       
        sch_enr_el_m,
        sch_enr_el_f,
        NULL::bigint as sch_enr_el_x,
        sch_enr_idea_m,
        sch_enr_idea_f,
        NULL::bigint as sch_enr_idea_x,
        sch_enr_504_m,
        sch_enr_504_f,
        NULL::bigint as sch_enr_504_x
    from staging.enrollment_2021
),

src_2022 as (
    select
        combokey,
        leaid as lea_id,
        survey_year,
        sch_enr_hi_m,
        sch_enr_hi_f,
        sch_enr_hi_x,
        sch_enr_am_m,
        sch_enr_am_f,
        sch_enr_am_x,
        sch_enr_as_m,
        sch_enr_as_f,
        sch_enr_as_x,
        sch_enr_hp_m,
        sch_enr_hp_f,
        sch_enr_hp_x,
        sch_enr_bl_m,
        sch_enr_bl_f,
        sch_enr_bl_x,
        sch_enr_wh_m,
        sch_enr_wh_f,
        sch_enr_wh_x,
        sch_enr_tr_m,
        sch_enr_tr_f,
        sch_enr_tr_x,
        tot_enr_m,
        tot_enr_f,
        tot_enr_x,       
        sch_enr_el_m,
        sch_enr_el_f,
        sch_enr_el_x,
        sch_enr_idea_m,
        sch_enr_idea_f,
        sch_enr_idea_x,
        sch_enr_504_m,
        sch_enr_504_f,
        sch_enr_504_x
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
            when 'sch_enr_hi_m' then u.sch_enr_hi_m
            when 'sch_enr_hi_f' then u.sch_enr_hi_f
            when 'sch_enr_hi_x' then u.sch_enr_hi_x
            when 'sch_enr_am_m' then u.sch_enr_am_m
            when 'sch_enr_am_f' then u.sch_enr_am_f
            when 'sch_enr_am_x' then u.sch_enr_am_x
            when 'sch_enr_as_m' then u.sch_enr_as_m
            when 'sch_enr_as_f' then u.sch_enr_as_f
            when 'sch_enr_as_x' then u.sch_enr_as_x
            when 'sch_enr_hp_m' then u.sch_enr_hp_m
            when 'sch_enr_hp_f' then u.sch_enr_hp_f
            when 'sch_enr_hp_x' then u.sch_enr_hp_x
            when 'sch_enr_bl_m' then u.sch_enr_bl_m
            when 'sch_enr_bl_f' then u.sch_enr_bl_f
            when 'sch_enr_bl_x' then u.sch_enr_bl_x
            when 'sch_enr_wh_m' then u.sch_enr_wh_m
            when 'sch_enr_wh_f' then u.sch_enr_wh_f
            when 'sch_enr_wh_x' then u.sch_enr_wh_x
            when 'sch_enr_tr_m' then u.sch_enr_tr_m
            when 'sch_enr_tr_f' then u.sch_enr_tr_f
            when 'sch_enr_tr_x' then u.sch_enr_tr_x
            when 'tot_enr_m' then u.tot_enr_m
            when 'tot_enr_f' then u.tot_enr_f
            when 'tot_enr_x' then u.tot_enr_x
            when 'sch_enr_el_m' then u.sch_enr_el_m
            when 'sch_enr_el_f' then u.sch_enr_el_f
            when 'sch_enr_el_x' then u.sch_enr_el_x
            when 'sch_enr_idea_m' then u.sch_enr_idea_m
            when 'sch_enr_idea_f' then u.sch_enr_idea_f
            when 'sch_enr_idea_x' then u.sch_enr_idea_x
            when 'sch_enr_504_m' then u.sch_enr_504_m
            when 'sch_enr_504_f' then u.sch_enr_504_f
            when 'sch_enr_504_x' then u.sch_enr_504_x
        end as enrollment_count
    from unioned u
    cross join (
        values
            ('sch_enr_hi_m', 'hispanic','male'),
            ('sch_enr_hi_f', 'hispanic','female'),
            ('sch_enr_hi_x', 'hispanic', 'nonbinary'),
            ('sch_enr_am_m', 'american_indian', 'male'),
            ('sch_enr_am_f', 'american_indian', 'female'),              
            ('sch_enr_am_x', 'american_indian', 'nonbinary'),
            ('sch_enr_as_m', 'asian', 'male'),                
            ('sch_enr_as_f', 'asian', 'female'),              
            ('sch_enr_as_x', 'asian', 'nonbinary'),
            ('sch_enr_hp_m', 'hawaiian_pacific_islander', 'male'),                
            ('sch_enr_hp_f', 'hawaiian_pacific_islander', 'female'),              
            ('sch_enr_hp_x', 'hawaiian_pacific_islander', 'nonbinary'),
            ('sch_enr_bl_m', 'black', 'male'),                
            ('sch_enr_bl_f', 'black', 'female'),
            ('sch_enr_bl_x', 'black', 'nonbinary'),           
            ('sch_enr_wh_m', 'white', 'male'),                
            ('sch_enr_wh_f', 'white', 'female'),
            ('sch_enr_wh_x', 'white', 'nonbinary'),           
            ('sch_enr_tr_m', 'two_or_more_races', 'male'),
            ('sch_enr_tr_f', 'two_or_more_races', 'female'),              
            ('sch_enr_tr_x', 'two_or_more_races', 'nonbinary'),           
            ('tot_enr_m', 'total', 'male'),
            ('tot_enr_f', 'total', 'female'),              
            ('tot_enr_x', 'total', 'nonbinary'),           
            ('sch_enr_el_m', 'english_learner', 'male'),
            ('sch_enr_el_f', 'english_learner', 'female'),              
            ('sch_enr_el_x', 'english_learner', 'nonbinary'),
            ('sch_enr_idea_m', 'idea', 'male'),                
            ('sch_enr_idea_f', 'idea', 'female'),              
            ('sch_enr_idea_x', 'idea', 'nonbinary'),
            ('sch_enr_504_m', 'section_504', 'male'),                
            ('sch_enr_504_f', 'section_504', 'female'),              
            ('sch_enr_504_x', 'section_504', 'nonbinary')
    ) as m(col_name, subgroup, sex)
)

select * from unpivoted