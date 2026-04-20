{{ config(materialized='table') }}

with src_2018 as (
    select
        combokey,
        leaid as lea_id,
        survey_year,
        -- EL enrollment (renaming)
        sch_lepenr_hi_m as elenr_hi_m,
        sch_lepenr_hi_f as elenr_hi_f,
        NULL::bigint as elenr_hi_x,
        sch_lepenr_am_m as elenr_am_m,
        sch_lepenr_am_f as elenr_am_f,
        NULL::bigint as elenr_am_x,
        sch_lepenr_as_m as elenr_as_m,
        sch_lepenr_as_f as elenr_as_f,
        NULL::bigint as elenr_as_x,
        sch_lepenr_hp_m as elenr_hp_m,
        sch_lepenr_hp_f as elenr_hp_f,
        NULL::bigint as elenr_hp_x,
        sch_lepenr_bl_m as elenr_bl_m,
        sch_lepenr_bl_f as elenr_bl_f,
        NULL::bigint as elenr_bl_x,
        sch_lepenr_wh_m as elenr_wh_m,
        sch_lepenr_wh_f as elenr_wh_f,
        NULL::bigint as elenr_wh_x,
        sch_lepenr_tr_m as elenr_tr_m,
        sch_lepenr_tr_f as elenr_tr_f,
        NULL::bigint as elenr_tr_x,
        tot_lepenr_m as tot_elenr_m,
        tot_lepenr_f as tot_elenr_f,
        NULL::bigint as tot_elenr_x,
        -- EL program (renaming)
        sch_lepprogenr_hi_m as elprogenr_hi_m,
        sch_lepprogenr_hi_f as elprogenr_hi_f,
        NULL::bigint as elprogenr_hi_x,
        sch_lepprogenr_am_m as elprogenr_am_m,
        sch_lepprogenr_am_f as elprogenr_am_f,
        NULL::bigint as elprogenr_am_x,
        sch_lepprogenr_as_m as elprogenr_as_m,
        sch_lepprogenr_as_f as elprogenr_as_f,
        NULL::bigint as elprogenr_as_x,
        sch_lepprogenr_hp_m as elprogenr_hp_m,
        sch_lepprogenr_hp_f as elprogenr_hp_f,
        NULL::bigint as elprogenr_hp_x,
        sch_lepprogenr_bl_m as elprogenr_bl_m,
        sch_lepprogenr_bl_f as elprogenr_bl_f,
        NULL::bigint as elprogenr_bl_x,
        sch_lepprogenr_wh_m as elprogenr_wh_m,
        sch_lepprogenr_wh_f as elprogenr_wh_f,
        NULL::bigint as elprogenr_wh_x,
        sch_lepprogenr_tr_m as elprogenr_tr_m,
        sch_lepprogenr_tr_f as elprogenr_tr_f,
        NULL::bigint as elprogenr_tr_x,
        sch_lepprogenr_idea_m as elprogenr_idea_m,
        sch_lepprogenr_idea_f as elprogenr_idea_f,
        NULL::bigint as elprogenr_idea_x,
        tot_lepprogenr_m as tot_elprogenr_m,
        tot_lepprogenr_f as tot_elprogenr_f,
        NULL::bigint as tot_elprogenr_x,
        -- IDEA enrollment
        sch_ideaenr_hi_m,
        sch_ideaenr_hi_f,
        NULL::bigint as sch_ideaenr_hi_x,
        sch_ideaenr_am_m,
        sch_ideaenr_am_f,
        NULL::bigint as sch_ideaenr_am_x,
        sch_ideaenr_as_m,
        sch_ideaenr_as_f,
        NULL::bigint as sch_ideaenr_as_x,
        sch_ideaenr_hp_m,
        sch_ideaenr_hp_f,
        NULL::bigint as sch_ideaenr_hp_x,
        sch_ideaenr_bl_m,
        sch_ideaenr_bl_f,
        NULL::bigint as sch_ideaenr_bl_x,
        sch_ideaenr_wh_m,
        sch_ideaenr_wh_f,
        NULL::bigint as sch_ideaenr_wh_x,
        sch_ideaenr_tr_m,
        sch_ideaenr_tr_f,
        NULL::bigint as sch_ideaenr_tr_x,
        sch_ideaenr_el_m,
        sch_ideaenr_el_f,
        NULL::bigint as sch_ideaenr_el_x,
        tot_ideaenr_m,
        tot_ideaenr_f,
        NULL::bigint as tot_ideaenr_x,
        -- Section 504 enrollment
        sch_504enr_hi_m,
        sch_504enr_hi_f,
        NULL::bigint as sch_504enr_hi_x,
        sch_504enr_am_m,
        sch_504enr_am_f,
        NULL::bigint as sch_504enr_am_x,
        sch_504enr_as_m,
        sch_504enr_as_f,
        NULL::bigint as sch_504enr_as_x,
        sch_504enr_hp_m,
        sch_504enr_hp_f,
        NULL::bigint as sch_504enr_hp_x,
        sch_504enr_bl_m,
        sch_504enr_bl_f,
        NULL::bigint as sch_504enr_bl_x,
        sch_504enr_wh_m,
        sch_504enr_wh_f,
        NULL::bigint as sch_504enr_wh_x,
        sch_504enr_tr_m,
        sch_504enr_tr_f,
        NULL::bigint as sch_504enr_tr_x,
        sch_504enr_el_m,
        sch_504enr_el_f,
        NULL::bigint as sch_504enr_el_x,
        tot_504enr_m,
        tot_504enr_f,
        NULL::bigint as tot_504enr_x
    from staging.enrollment_2018
),

src_2021 as (
    select
        combokey,
        leaid as lea_id,
        survey_year,
        -- EL enrollment (renaming)
        sch_lepenr_hi_m as elenr_hi_m,
        sch_lepenr_hi_f as elenr_hi_f,
        NULL::bigint as elenr_hi_x,
        sch_lepenr_am_m as elenr_am_m,
        sch_lepenr_am_f as elenr_am_f,
        NULL::bigint as elenr_am_x,
        sch_lepenr_as_m as elenr_as_m,
        sch_lepenr_as_f as elenr_as_f,
        NULL::bigint as elenr_as_x,
        sch_lepenr_hp_m as elenr_hp_m,
        sch_lepenr_hp_f as elenr_hp_f,
        NULL::bigint as elenr_hp_x,
        sch_lepenr_bl_m as elenr_bl_m,
        sch_lepenr_bl_f as elenr_bl_f,
        NULL::bigint as elenr_bl_x,
        sch_lepenr_wh_m as elenr_wh_m,
        sch_lepenr_wh_f as elenr_wh_f,
        NULL::bigint as elenr_wh_x,
        sch_lepenr_tr_m as elenr_tr_m,
        sch_lepenr_tr_f as elenr_tr_f,
        NULL::bigint as elenr_tr_x,
        tot_lepenr_m as tot_elenr_m,
        tot_lepenr_f as tot_elenr_f,
        NULL::bigint as tot_elenr_x,
        -- EL program (renaming)
        sch_lepprogenr_hi_m as elprogenr_hi_m,
        sch_lepprogenr_hi_f as elprogenr_hi_f,
        NULL::bigint as elprogenr_hi_x,
        sch_lepprogenr_am_m as elprogenr_am_m,
        sch_lepprogenr_am_f as elprogenr_am_f,
        NULL::bigint as elprogenr_am_x,
        sch_lepprogenr_as_m as elprogenr_as_m,
        sch_lepprogenr_as_f as elprogenr_as_f,
        NULL::bigint as elprogenr_as_x,
        sch_lepprogenr_hp_m as elprogenr_hp_m,
        sch_lepprogenr_hp_f as elprogenr_hp_f,
        NULL::bigint as elprogenr_hp_x,
        sch_lepprogenr_bl_m as elprogenr_bl_m,
        sch_lepprogenr_bl_f as elprogenr_bl_f,
        NULL::bigint as elprogenr_bl_x,
        sch_lepprogenr_wh_m as elprogenr_wh_m,
        sch_lepprogenr_wh_f as elprogenr_wh_f,
        NULL::bigint as elprogenr_wh_x,
        sch_lepprogenr_tr_m as elprogenr_tr_m,
        sch_lepprogenr_tr_f as elprogenr_tr_f,
        NULL::bigint as elprogenr_tr_x,
        NULL::bigint as elprogenr_idea_m,
        NULL::bigint as elprogenr_idea_f,
        NULL::bigint as elprogenr_idea_x,
        tot_lepprogenr_m as tot_elprogenr_m,
        tot_lepprogenr_f as tot_elprogenr_f,
        NULL::bigint as tot_elprogenr_x,
        -- IDEA enrollment
        sch_ideaenr_hi_m,
        sch_ideaenr_hi_f,
        NULL::bigint as sch_ideaenr_hi_x,
        sch_ideaenr_am_m,
        sch_ideaenr_am_f,
        NULL::bigint as sch_ideaenr_am_x,
        sch_ideaenr_as_m,
        sch_ideaenr_as_f,
        NULL::bigint as sch_ideaenr_as_x,
        sch_ideaenr_hp_m,
        sch_ideaenr_hp_f,
        NULL::bigint as sch_ideaenr_hp_x,
        sch_ideaenr_bl_m,
        sch_ideaenr_bl_f,
        NULL::bigint as sch_ideaenr_bl_x,
        sch_ideaenr_wh_m,
        sch_ideaenr_wh_f,
        NULL::bigint as sch_ideaenr_wh_x,
        sch_ideaenr_tr_m,
        sch_ideaenr_tr_f,
        NULL::bigint as sch_ideaenr_tr_x,
        sch_ideaenr_el_m,
        sch_ideaenr_el_f,
        NULL::bigint as sch_ideaenr_el_x,
        tot_ideaenr_m,
        tot_ideaenr_f,
        NULL::bigint as tot_ideaenr_x,
        -- Section 504 enrollment
        sch_504enr_hi_m,
        sch_504enr_hi_f,
        NULL::bigint as sch_504enr_hi_x,
        sch_504enr_am_m,
        sch_504enr_am_f,
        NULL::bigint as sch_504enr_am_x,
        sch_504enr_as_m,
        sch_504enr_as_f,
        NULL::bigint as sch_504enr_as_x,
        sch_504enr_hp_m,
        sch_504enr_hp_f,
        NULL::bigint as sch_504enr_hp_x,
        sch_504enr_bl_m,
        sch_504enr_bl_f,
        NULL::bigint as sch_504enr_bl_x,
        sch_504enr_wh_m,
        sch_504enr_wh_f,
        NULL::bigint as sch_504enr_wh_x,
        sch_504enr_tr_m,
        sch_504enr_tr_f,
        NULL::bigint as sch_504enr_tr_x,
        sch_504enr_el_m,
        sch_504enr_el_f,
        NULL::bigint as sch_504enr_el_x,
        tot_504enr_m,
        tot_504enr_f,
        NULL::bigint as tot_504enr_x
    from staging.enrollment_2021
),

src_2022 as (
    select
        combokey,
        leaid as lea_id,
        survey_year,
        -- EL enrollment
        sch_elenr_hi_m as elenr_hi_m,
        sch_elenr_hi_f as elenr_hi_f,
        sch_elenr_hi_x as elenr_hi_x,
        sch_elenr_am_m as elenr_am_m,
        sch_elenr_am_f as elenr_am_f,
        sch_elenr_am_x as elenr_am_x,
        sch_elenr_as_m as elenr_as_m,
        sch_elenr_as_f as elenr_as_f,
        sch_elenr_as_x as elenr_as_x,
        sch_elenr_hp_m as elenr_hp_m,
        sch_elenr_hp_f as elenr_hp_f,
        sch_elenr_hp_x as elenr_hp_x,
        sch_elenr_bl_m as elenr_bl_m,
        sch_elenr_bl_f as elenr_bl_f,
        sch_elenr_bl_x as elenr_bl_x,
        sch_elenr_wh_m as elenr_wh_m,
        sch_elenr_wh_f as elenr_wh_f,
        sch_elenr_wh_x as elenr_wh_x,
        sch_elenr_tr_m as elenr_tr_m,
        sch_elenr_tr_f as elenr_tr_f,
        sch_elenr_tr_x as elenr_tr_x,
        tot_elenr_m,
        tot_elenr_f,
        tot_elenr_x,
        -- EL program (no idea subgroup in 2022)
        sch_elprogenr_hi_m as elprogenr_hi_m,
        sch_elprogenr_hi_f as elprogenr_hi_f,
        sch_elprogenr_hi_x as elprogenr_hi_x,
        sch_elprogenr_am_m as elprogenr_am_m,
        sch_elprogenr_am_f as elprogenr_am_f,
        sch_elprogenr_am_x as elprogenr_am_x,
        sch_elprogenr_as_m as elprogenr_as_m,
        sch_elprogenr_as_f as elprogenr_as_f,
        sch_elprogenr_as_x as elprogenr_as_x,
        sch_elprogenr_hp_m as elprogenr_hp_m,
        sch_elprogenr_hp_f as elprogenr_hp_f,
        sch_elprogenr_hp_x as elprogenr_hp_x,
        sch_elprogenr_bl_m as elprogenr_bl_m,
        sch_elprogenr_bl_f as elprogenr_bl_f,
        sch_elprogenr_bl_x as elprogenr_bl_x,
        sch_elprogenr_wh_m as elprogenr_wh_m,
        sch_elprogenr_wh_f as elprogenr_wh_f,
        sch_elprogenr_wh_x as elprogenr_wh_x,
        sch_elprogenr_tr_m as elprogenr_tr_m,
        sch_elprogenr_tr_f as elprogenr_tr_f,
        sch_elprogenr_tr_x as elprogenr_tr_x,
        NULL::bigint as elprogenr_idea_m,
        NULL::bigint as elprogenr_idea_f,
        NULL::bigint as elprogenr_idea_x,
        tot_elprogenr_m,
        tot_elprogenr_f,
        tot_elprogenr_x,
        -- IDEA enrollment
        sch_ideaenr_hi_m,
        sch_ideaenr_hi_f,
        sch_ideaenr_hi_x,
        sch_ideaenr_am_m,
        sch_ideaenr_am_f,
        sch_ideaenr_am_x,
        sch_ideaenr_as_m,
        sch_ideaenr_as_f,
        sch_ideaenr_as_x,
        sch_ideaenr_hp_m,
        sch_ideaenr_hp_f,
        sch_ideaenr_hp_x,
        sch_ideaenr_bl_m,
        sch_ideaenr_bl_f,
        sch_ideaenr_bl_x,
        sch_ideaenr_wh_m,
        sch_ideaenr_wh_f,
        sch_ideaenr_wh_x,
        sch_ideaenr_tr_m,
        sch_ideaenr_tr_f,
        sch_ideaenr_tr_x,
        sch_ideaenr_el_m,
        sch_ideaenr_el_f,
        sch_ideaenr_el_x,
        tot_ideaenr_m,
        tot_ideaenr_f,
        tot_ideaenr_x,
        -- Section 504 enrollment
        sch_504enr_hi_m,
        sch_504enr_hi_f,
        sch_504enr_hi_x,
        sch_504enr_am_m,
        sch_504enr_am_f,
        sch_504enr_am_x,
        sch_504enr_as_m,
        sch_504enr_as_f,
        sch_504enr_as_x,
        sch_504enr_hp_m,
        sch_504enr_hp_f,
        sch_504enr_hp_x,
        sch_504enr_bl_m,
        sch_504enr_bl_f,
        sch_504enr_bl_x,
        sch_504enr_wh_m,
        sch_504enr_wh_f,
        sch_504enr_wh_x,
        sch_504enr_tr_m,
        sch_504enr_tr_f,
        sch_504enr_tr_x,
        sch_504enr_el_m,
        sch_504enr_el_f,
        sch_504enr_el_x,
        tot_504enr_m,
        tot_504enr_f,
        tot_504enr_x
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
        m.program,
        m.subgroup,
        m.sex,
        case m.col_name
            -- EL enrollment
            when 'elenr_hi_m' then u.elenr_hi_m
            when 'elenr_hi_f' then u.elenr_hi_f
            when 'elenr_hi_x' then u.elenr_hi_x
            when 'elenr_am_m' then u.elenr_am_m
            when 'elenr_am_f' then u.elenr_am_f
            when 'elenr_am_x' then u.elenr_am_x
            when 'elenr_as_m' then u.elenr_as_m
            when 'elenr_as_f' then u.elenr_as_f
            when 'elenr_as_x' then u.elenr_as_x
            when 'elenr_hp_m' then u.elenr_hp_m
            when 'elenr_hp_f' then u.elenr_hp_f
            when 'elenr_hp_x' then u.elenr_hp_x
            when 'elenr_bl_m' then u.elenr_bl_m
            when 'elenr_bl_f' then u.elenr_bl_f
            when 'elenr_bl_x' then u.elenr_bl_x
            when 'elenr_wh_m' then u.elenr_wh_m
            when 'elenr_wh_f' then u.elenr_wh_f
            when 'elenr_wh_x' then u.elenr_wh_x
            when 'elenr_tr_m' then u.elenr_tr_m
            when 'elenr_tr_f' then u.elenr_tr_f
            when 'elenr_tr_x' then u.elenr_tr_x
            when 'tot_elenr_m' then u.tot_elenr_m
            when 'tot_elenr_f' then u.tot_elenr_f
            when 'tot_elenr_x' then u.tot_elenr_x
            -- EL program
            when 'elprogenr_hi_m' then u.elprogenr_hi_m
            when 'elprogenr_hi_f' then u.elprogenr_hi_f
            when 'elprogenr_hi_x' then u.elprogenr_hi_x
            when 'elprogenr_am_m' then u.elprogenr_am_m
            when 'elprogenr_am_f' then u.elprogenr_am_f
            when 'elprogenr_am_x' then u.elprogenr_am_x
            when 'elprogenr_as_m' then u.elprogenr_as_m
            when 'elprogenr_as_f' then u.elprogenr_as_f
            when 'elprogenr_as_x' then u.elprogenr_as_x
            when 'elprogenr_hp_m' then u.elprogenr_hp_m
            when 'elprogenr_hp_f' then u.elprogenr_hp_f
            when 'elprogenr_hp_x' then u.elprogenr_hp_x
            when 'elprogenr_bl_m' then u.elprogenr_bl_m
            when 'elprogenr_bl_f' then u.elprogenr_bl_f
            when 'elprogenr_bl_x' then u.elprogenr_bl_x
            when 'elprogenr_wh_m' then u.elprogenr_wh_m
            when 'elprogenr_wh_f' then u.elprogenr_wh_f
            when 'elprogenr_wh_x' then u.elprogenr_wh_x
            when 'elprogenr_tr_m' then u.elprogenr_tr_m
            when 'elprogenr_tr_f' then u.elprogenr_tr_f
            when 'elprogenr_tr_x' then u.elprogenr_tr_x
            when 'elprogenr_idea_m' then u.elprogenr_idea_m
            when 'elprogenr_idea_f' then u.elprogenr_idea_f
            when 'elprogenr_idea_x' then u.elprogenr_idea_x
            when 'tot_elprogenr_m' then u.tot_elprogenr_m
            when 'tot_elprogenr_f' then u.tot_elprogenr_f
            when 'tot_elprogenr_x' then u.tot_elprogenr_x
            -- IDEA
            when 'sch_ideaenr_hi_m' then u.sch_ideaenr_hi_m
            when 'sch_ideaenr_hi_f' then u.sch_ideaenr_hi_f
            when 'sch_ideaenr_hi_x' then u.sch_ideaenr_hi_x
            when 'sch_ideaenr_am_m' then u.sch_ideaenr_am_m
            when 'sch_ideaenr_am_f' then u.sch_ideaenr_am_f
            when 'sch_ideaenr_am_x' then u.sch_ideaenr_am_x
            when 'sch_ideaenr_as_m' then u.sch_ideaenr_as_m
            when 'sch_ideaenr_as_f' then u.sch_ideaenr_as_f
            when 'sch_ideaenr_as_x' then u.sch_ideaenr_as_x
            when 'sch_ideaenr_hp_m' then u.sch_ideaenr_hp_m
            when 'sch_ideaenr_hp_f' then u.sch_ideaenr_hp_f
            when 'sch_ideaenr_hp_x' then u.sch_ideaenr_hp_x
            when 'sch_ideaenr_bl_m' then u.sch_ideaenr_bl_m
            when 'sch_ideaenr_bl_f' then u.sch_ideaenr_bl_f
            when 'sch_ideaenr_bl_x' then u.sch_ideaenr_bl_x
            when 'sch_ideaenr_wh_m' then u.sch_ideaenr_wh_m
            when 'sch_ideaenr_wh_f' then u.sch_ideaenr_wh_f
            when 'sch_ideaenr_wh_x' then u.sch_ideaenr_wh_x
            when 'sch_ideaenr_tr_m' then u.sch_ideaenr_tr_m
            when 'sch_ideaenr_tr_f' then u.sch_ideaenr_tr_f
            when 'sch_ideaenr_tr_x' then u.sch_ideaenr_tr_x
            when 'sch_ideaenr_el_m' then u.sch_ideaenr_el_m
            when 'sch_ideaenr_el_f' then u.sch_ideaenr_el_f
            when 'sch_ideaenr_el_x' then u.sch_ideaenr_el_x
            when 'tot_ideaenr_m' then u.tot_ideaenr_m
            when 'tot_ideaenr_f' then u.tot_ideaenr_f
            when 'tot_ideaenr_x' then u.tot_ideaenr_x
            -- Section 504
            when 'sch_504enr_hi_m' then u.sch_504enr_hi_m
            when 'sch_504enr_hi_f' then u.sch_504enr_hi_f
            when 'sch_504enr_hi_x' then u.sch_504enr_hi_x
            when 'sch_504enr_am_m' then u.sch_504enr_am_m
            when 'sch_504enr_am_f' then u.sch_504enr_am_f
            when 'sch_504enr_am_x' then u.sch_504enr_am_x
            when 'sch_504enr_as_m' then u.sch_504enr_as_m
            when 'sch_504enr_as_f' then u.sch_504enr_as_f
            when 'sch_504enr_as_x' then u.sch_504enr_as_x
            when 'sch_504enr_hp_m' then u.sch_504enr_hp_m
            when 'sch_504enr_hp_f' then u.sch_504enr_hp_f
            when 'sch_504enr_hp_x' then u.sch_504enr_hp_x
            when 'sch_504enr_bl_m' then u.sch_504enr_bl_m
            when 'sch_504enr_bl_f' then u.sch_504enr_bl_f
            when 'sch_504enr_bl_x' then u.sch_504enr_bl_x
            when 'sch_504enr_wh_m' then u.sch_504enr_wh_m
            when 'sch_504enr_wh_f' then u.sch_504enr_wh_f
            when 'sch_504enr_wh_x' then u.sch_504enr_wh_x
            when 'sch_504enr_tr_m' then u.sch_504enr_tr_m
            when 'sch_504enr_tr_f' then u.sch_504enr_tr_f
            when 'sch_504enr_tr_x' then u.sch_504enr_tr_x
            when 'sch_504enr_el_m' then u.sch_504enr_el_m
            when 'sch_504enr_el_f' then u.sch_504enr_el_f
            when 'sch_504enr_el_x' then u.sch_504enr_el_x
            when 'tot_504enr_m' then u.tot_504enr_m
            when 'tot_504enr_f' then u.tot_504enr_f
            when 'tot_504enr_x' then u.tot_504enr_x
        end as enrollment_count
    from unioned u
    cross join (
        values
            -- EL enrollment
            ('elenr_hi_m', 'el', 'hispanic', 'male'),
            ('elenr_hi_f', 'el', 'hispanic', 'female'),
            ('elenr_hi_x', 'el', 'hispanic', 'nonbinary'),
            ('elenr_am_m', 'el', 'american_indian', 'male'),
            ('elenr_am_f', 'el', 'american_indian', 'female'),
            ('elenr_am_x', 'el', 'american_indian', 'nonbinary'),
            ('elenr_as_m', 'el', 'asian', 'male'),
            ('elenr_as_f', 'el', 'asian', 'female'),
            ('elenr_as_x', 'el', 'asian', 'nonbinary'),
            ('elenr_hp_m', 'el', 'hawaiian_pacific_islander', 'male'),
            ('elenr_hp_f', 'el', 'hawaiian_pacific_islander', 'female'),
            ('elenr_hp_x', 'el', 'hawaiian_pacific_islander', 'nonbinary'),
            ('elenr_bl_m', 'el', 'black', 'male'),
            ('elenr_bl_f', 'el', 'black', 'female'),
            ('elenr_bl_x', 'el', 'black', 'nonbinary'),
            ('elenr_wh_m', 'el', 'white', 'male'),
            ('elenr_wh_f', 'el', 'white', 'female'),
            ('elenr_wh_x', 'el', 'white', 'nonbinary'),
            ('elenr_tr_m', 'el', 'two_or_more_races', 'male'),
            ('elenr_tr_f', 'el', 'two_or_more_races', 'female'),
            ('elenr_tr_x', 'el', 'two_or_more_races', 'nonbinary'),
            ('tot_elenr_m', 'el', 'total', 'male'),
            ('tot_elenr_f', 'el', 'total', 'female'),
            ('tot_elenr_x', 'el', 'total', 'nonbinary'),
            -- EL program
            ('elprogenr_hi_m', 'el_program', 'hispanic', 'male'),
            ('elprogenr_hi_f', 'el_program', 'hispanic', 'female'),
            ('elprogenr_hi_x', 'el_program', 'hispanic', 'nonbinary'),
            ('elprogenr_am_m', 'el_program', 'american_indian', 'male'),
            ('elprogenr_am_f', 'el_program', 'american_indian', 'female'),
            ('elprogenr_am_x', 'el_program', 'american_indian', 'nonbinary'),
            ('elprogenr_as_m', 'el_program', 'asian', 'male'),
            ('elprogenr_as_f', 'el_program', 'asian', 'female'),
            ('elprogenr_as_x', 'el_program', 'asian', 'nonbinary'),
            ('elprogenr_hp_m', 'el_program', 'hawaiian_pacific_islander', 'male'),
            ('elprogenr_hp_f', 'el_program', 'hawaiian_pacific_islander', 'female'),
            ('elprogenr_hp_x', 'el_program', 'hawaiian_pacific_islander', 'nonbinary'),
            ('elprogenr_bl_m', 'el_program', 'black', 'male'),
            ('elprogenr_bl_f', 'el_program', 'black', 'female'),
            ('elprogenr_bl_x', 'el_program', 'black', 'nonbinary'),
            ('elprogenr_wh_m', 'el_program', 'white', 'male'),
            ('elprogenr_wh_f', 'el_program', 'white', 'female'),
            ('elprogenr_wh_x', 'el_program', 'white', 'nonbinary'),
            ('elprogenr_tr_m', 'el_program', 'two_or_more_races', 'male'),
            ('elprogenr_tr_f', 'el_program', 'two_or_more_races', 'female'),
            ('elprogenr_tr_x', 'el_program', 'two_or_more_races', 'nonbinary'),
            ('elprogenr_idea_m', 'el_program', 'idea', 'male'),
            ('elprogenr_idea_f', 'el_program', 'idea', 'female'),
            ('elprogenr_idea_x', 'el_program', 'idea', 'nonbinary'),
            ('tot_elprogenr_m', 'el_program', 'total', 'male'),
            ('tot_elprogenr_f', 'el_program', 'total', 'female'),
            ('tot_elprogenr_x', 'el_program', 'total', 'nonbinary'),
            -- IDEA
            ('sch_ideaenr_hi_m', 'idea', 'hispanic', 'male'),
            ('sch_ideaenr_hi_f', 'idea', 'hispanic', 'female'),
            ('sch_ideaenr_hi_x', 'idea', 'hispanic', 'nonbinary'),
            ('sch_ideaenr_am_m', 'idea', 'american_indian', 'male'),
            ('sch_ideaenr_am_f', 'idea', 'american_indian', 'female'),
            ('sch_ideaenr_am_x', 'idea', 'american_indian', 'nonbinary'),
            ('sch_ideaenr_as_m', 'idea', 'asian', 'male'),
            ('sch_ideaenr_as_f', 'idea', 'asian', 'female'),
            ('sch_ideaenr_as_x', 'idea', 'asian', 'nonbinary'),
            ('sch_ideaenr_hp_m', 'idea', 'hawaiian_pacific_islander', 'male'),
            ('sch_ideaenr_hp_f', 'idea', 'hawaiian_pacific_islander', 'female'),
            ('sch_ideaenr_hp_x', 'idea', 'hawaiian_pacific_islander', 'nonbinary'),
            ('sch_ideaenr_bl_m', 'idea', 'black', 'male'),
            ('sch_ideaenr_bl_f', 'idea', 'black', 'female'),
            ('sch_ideaenr_bl_x', 'idea', 'black', 'nonbinary'),
            ('sch_ideaenr_wh_m', 'idea', 'white', 'male'),
            ('sch_ideaenr_wh_f', 'idea', 'white', 'female'),
            ('sch_ideaenr_wh_x', 'idea', 'white', 'nonbinary'),
            ('sch_ideaenr_tr_m', 'idea', 'two_or_more_races', 'male'),
            ('sch_ideaenr_tr_f', 'idea', 'two_or_more_races', 'female'),
            ('sch_ideaenr_tr_x', 'idea', 'two_or_more_races', 'nonbinary'),
            ('sch_ideaenr_el_m', 'idea', 'english_learner', 'male'),
            ('sch_ideaenr_el_f', 'idea',  'english_learner', 'female'),
            ('sch_ideaenr_el_x', 'idea', 'english_learner', 'nonbinary'),
            ('tot_ideaenr_m', 'idea', 'total', 'male'),
            ('tot_ideaenr_f', 'idea', 'total', 'female'),
            ('tot_ideaenr_x', 'idea', 'total', 'nonbinary'),
            -- Section 504
            ('sch_504enr_hi_m', 'section_504', 'hispanic', 'male'),
            ('sch_504enr_hi_f', 'section_504', 'hispanic', 'female'),
            ('sch_504enr_hi_x', 'section_504', 'hispanic', 'nonbinary'),
            ('sch_504enr_am_m', 'section_504', 'american_indian', 'male'),
            ('sch_504enr_am_f', 'section_504', 'american_indian', 'female'),
            ('sch_504enr_am_x', 'section_504', 'american_indian', 'nonbinary'),
            ('sch_504enr_as_m', 'section_504', 'asian', 'male'),
            ('sch_504enr_as_f', 'section_504', 'asian', 'female'),
            ('sch_504enr_as_x', 'section_504', 'asian', 'nonbinary'),
            ('sch_504enr_hp_m', 'section_504', 'hawaiian_pacific_islander', 'male'),
            ('sch_504enr_hp_f', 'section_504', 'hawaiian_pacific_islander', 'female'),
            ('sch_504enr_hp_x', 'section_504', 'hawaiian_pacific_islander', 'nonbinary'),
            ('sch_504enr_bl_m', 'section_504', 'black', 'male'),
            ('sch_504enr_bl_f', 'section_504', 'black', 'female'),
            ('sch_504enr_bl_x', 'section_504', 'black', 'nonbinary'),
            ('sch_504enr_wh_m', 'section_504', 'white', 'male'),
            ('sch_504enr_wh_f', 'section_504', 'white', 'female'),
            ('sch_504enr_wh_x', 'section_504', 'white', 'nonbinary'),
            ('sch_504enr_tr_m', 'section_504', 'two_or_more_races', 'male'),
            ('sch_504enr_tr_f', 'section_504', 'two_or_more_races', 'female'),
            ('sch_504enr_tr_x', 'section_504', 'two_or_more_races', 'nonbinary'),
            ('sch_504enr_el_m', 'section_504', 'english_learner', 'male'),
            ('sch_504enr_el_f', 'section_504', 'english_learner', 'female'),
            ('sch_504enr_el_x', 'section_504', 'english_learner', 'nonbinary'),
            ('tot_504enr_m', 'section_504', 'total', 'male'),
            ('tot_504enr_f', 'section_504', 'total', 'female'),
            ('tot_504enr_x', 'section_504', 'total', 'nonbinary')
    ) as m(col_name, program, subgroup, sex)
)

select * from unpivoted