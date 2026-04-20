{{ config(materialized='table') }}

with src_2022 as (
    select
        combokey,
        leaid as lea_id,
        survey_year,
        -- EL enrollment                                         
        sch_pselenr_am_m,
        sch_pselenr_am_f,
        sch_pselenr_as_f,
        sch_pselenr_as_m,
        sch_pselenr_bl_f,
        sch_pselenr_bl_m,
        sch_pselenr_hi_f,
        sch_pselenr_hi_m,
        sch_pselenr_hp_f,
        sch_pselenr_hp_m,
        sch_pselenr_tr_f,
        sch_pselenr_tr_m,
        sch_pselenr_wh_f,
        sch_pselenr_wh_m,
        tot_pselenr_f,
        tot_pselenr_m,
        -- EL program
        sch_pselprogenr_am_f,
        sch_pselprogenr_am_m,
        sch_pselprogenr_as_f,
        sch_pselprogenr_as_m,
        sch_pselprogenr_bl_f,
        sch_pselprogenr_bl_m,
        sch_pselprogenr_hi_f,
        sch_pselprogenr_hi_m,
        sch_pselprogenr_hp_f,
        sch_pselprogenr_hp_m,
        sch_pselprogenr_tr_f,
        sch_pselprogenr_tr_m,
        sch_pselprogenr_wh_f,
        sch_pselprogenr_wh_m,
        tot_pselprogenr_f,
        tot_pselprogenr_m,
        -- IDEA
        sch_psideaenr_am_f,
        sch_psideaenr_am_m,
        sch_psideaenr_as_f,
        sch_psideaenr_as_m,
        sch_psideaenr_bl_f,
        sch_psideaenr_bl_m,
        sch_psideaenr_el_f,
        sch_psideaenr_el_m,
        sch_psideaenr_hi_f,
        sch_psideaenr_hi_m,
        sch_psideaenr_hp_f,
        sch_psideaenr_hp_m,
        sch_psideaenr_tr_f,
        sch_psideaenr_tr_m,
        sch_psideaenr_wh_f,
        sch_psideaenr_wh_m,
        tot_psideaenr_f,
        tot_psideaenr_m,
        -- Section 504
        sch_ps504enr_am_f,
        sch_ps504enr_am_m,
        sch_ps504enr_as_f,
        sch_ps504enr_as_m,
        sch_ps504enr_bl_f,
        sch_ps504enr_bl_m,
        sch_ps504enr_el_f,
        sch_ps504enr_el_m,
        sch_ps504enr_hi_f,
        sch_ps504enr_hi_m,
        sch_ps504enr_hp_f,
        sch_ps504enr_hp_m,
        sch_ps504enr_tr_f,
        sch_ps504enr_tr_m,
        sch_ps504enr_wh_f,
        sch_ps504enr_wh_m,
        tot_ps504enr_f,
        tot_ps504enr_m
    from staging.enrollment_2022
),

unpivoted as (
    select
        combokey,
        lea_id,
        survey_year,
        m.program,
        m.subgroup,
        m.sex,
        case m.col_name
            -- EL enrollment
            when 'sch_pselenr_hi_m' then s.sch_pselenr_hi_m                         
            when 'sch_pselenr_hi_f' then s.sch_pselenr_hi_f
            when 'sch_pselenr_am_m' then s.sch_pselenr_am_m                         
            when 'sch_pselenr_am_f' then s.sch_pselenr_am_f
            when 'sch_pselenr_as_m' then s.sch_pselenr_as_m
            when 'sch_pselenr_as_f' then s.sch_pselenr_as_f                         
            when 'sch_pselenr_hp_m' then s.sch_pselenr_hp_m
            when 'sch_pselenr_hp_f' then s.sch_pselenr_hp_f                         
            when 'sch_pselenr_bl_m' then s.sch_pselenr_bl_m
            when 'sch_pselenr_bl_f' then s.sch_pselenr_bl_f                         
            when 'sch_pselenr_wh_m' then s.sch_pselenr_wh_m
            when 'sch_pselenr_wh_f' then s.sch_pselenr_wh_f                         
            when 'sch_pselenr_tr_m' then s.sch_pselenr_tr_m
            when 'sch_pselenr_tr_f' then s.sch_pselenr_tr_f                         
            when 'tot_pselenr_m' then s.tot_pselenr_m
            when 'tot_pselenr_f' then s.tot_pselenr_f                               
            -- EL program
            when 'sch_pselprogenr_hi_m' then s.sch_pselprogenr_hi_m                 
            when 'sch_pselprogenr_hi_f' then s.sch_pselprogenr_hi_f
            when 'sch_pselprogenr_am_m' then s.sch_pselprogenr_am_m                 
            when 'sch_pselprogenr_am_f' then s.sch_pselprogenr_am_f                 
            when 'sch_pselprogenr_as_m' then s.sch_pselprogenr_as_m
            when 'sch_pselprogenr_as_f' then s.sch_pselprogenr_as_f                 
            when 'sch_pselprogenr_hp_m' then s.sch_pselprogenr_hp_m
            when 'sch_pselprogenr_hp_f' then s.sch_pselprogenr_hp_f                 
            when 'sch_pselprogenr_bl_m' then s.sch_pselprogenr_bl_m
            when 'sch_pselprogenr_bl_f' then s.sch_pselprogenr_bl_f                 
            when 'sch_pselprogenr_wh_m' then s.sch_pselprogenr_wh_m
            when 'sch_pselprogenr_wh_f' then s.sch_pselprogenr_wh_f                 
            when 'sch_pselprogenr_tr_m' then s.sch_pselprogenr_tr_m
            when 'sch_pselprogenr_tr_f' then s.sch_pselprogenr_tr_f                 
            when 'tot_pselprogenr_m' then s.tot_pselprogenr_m
            when 'tot_pselprogenr_f' then s.tot_pselprogenr_f                       
            -- IDEA                         
            when 'sch_psideaenr_hi_m' then s.sch_psideaenr_hi_m                     
            when 'sch_psideaenr_hi_f' then s.sch_psideaenr_hi_f
            when 'sch_psideaenr_am_m' then s.sch_psideaenr_am_m                     
            when 'sch_psideaenr_am_f' then s.sch_psideaenr_am_f
            when 'sch_psideaenr_as_m' then s.sch_psideaenr_as_m                     
            when 'sch_psideaenr_as_f' then s.sch_psideaenr_as_f                     
            when 'sch_psideaenr_hp_m' then s.sch_psideaenr_hp_m
            when 'sch_psideaenr_hp_f' then s.sch_psideaenr_hp_f                     
            when 'sch_psideaenr_bl_m' then s.sch_psideaenr_bl_m
            when 'sch_psideaenr_bl_f' then s.sch_psideaenr_bl_f                     
            when 'sch_psideaenr_wh_m' then s.sch_psideaenr_wh_m
            when 'sch_psideaenr_wh_f' then s.sch_psideaenr_wh_f                     
            when 'sch_psideaenr_tr_m' then s.sch_psideaenr_tr_m
            when 'sch_psideaenr_tr_f' then s.sch_psideaenr_tr_f                     
            when 'sch_psideaenr_el_m' then s.sch_psideaenr_el_m
            when 'sch_psideaenr_el_f' then s.sch_psideaenr_el_f                     
            when 'tot_psideaenr_m' then s.tot_psideaenr_m                           
            when 'tot_psideaenr_f' then s.tot_psideaenr_f                           
            -- Section 504                                                          
            when 'sch_ps504enr_hi_m' then s.sch_ps504enr_hi_m
            when 'sch_ps504enr_hi_f' then s.sch_ps504enr_hi_f                       
            when 'sch_ps504enr_am_m' then s.sch_ps504enr_am_m                       
            when 'sch_ps504enr_am_f' then s.sch_ps504enr_am_f
            when 'sch_ps504enr_as_m' then s.sch_ps504enr_as_m                       
            when 'sch_ps504enr_as_f' then s.sch_ps504enr_as_f                       
            when 'sch_ps504enr_hp_m' then s.sch_ps504enr_hp_m
            when 'sch_ps504enr_hp_f' then s.sch_ps504enr_hp_f                       
            when 'sch_ps504enr_bl_m' then s.sch_ps504enr_bl_m
            when 'sch_ps504enr_bl_f' then s.sch_ps504enr_bl_f                       
            when 'sch_ps504enr_wh_m' then s.sch_ps504enr_wh_m
            when 'sch_ps504enr_wh_f' then s.sch_ps504enr_wh_f                       
            when 'sch_ps504enr_tr_m' then s.sch_ps504enr_tr_m
            when 'sch_ps504enr_tr_f' then s.sch_ps504enr_tr_f                       
            when 'sch_ps504enr_el_m' then s.sch_ps504enr_el_m
            when 'sch_ps504enr_el_f' then s.sch_ps504enr_el_f                       
            when 'tot_ps504enr_m' then s.tot_ps504enr_m
            when 'tot_ps504enr_f' then s.tot_ps504enr_f                             
        end as enrollment_count             
    from src_2022 s                                                                 
    cross join (
        values                                                                      
            -- EL enrollment
            ('sch_pselenr_hi_m', 'el', 'hispanic', 'male'),                         
            ('sch_pselenr_hi_f', 'el', 'hispanic', 'female'),
            ('sch_pselenr_am_m', 'el', 'american_indian', 'male'),
            ('sch_pselenr_am_f', 'el', 'american_indian', 'female'),
            ('sch_pselenr_as_m', 'el', 'asian', 'male'),                            
            ('sch_pselenr_as_f', 'el', 'asian', 'female'),
            ('sch_pselenr_hp_m', 'el', 'hawaiian_pacific_islander', 'male'),        
            ('sch_pselenr_hp_f', 'el', 'hawaiian_pacific_islander', 'female'),
            ('sch_pselenr_bl_m', 'el', 'black', 'male'),                            
            ('sch_pselenr_bl_f', 'el', 'black', 'female'),                          
            ('sch_pselenr_wh_m', 'el', 'white', 'male'),                            
            ('sch_pselenr_wh_f', 'el', 'white', 'female'),                          
            ('sch_pselenr_tr_m', 'el', 'two_or_more_races', 'male'),                
            ('sch_pselenr_tr_f', 'el', 'two_or_more_races', 'female'),
            ('tot_pselenr_m', 'el', 'total', 'male'),                               
            ('tot_pselenr_f', 'el', 'total', 'female'),
            -- EL program                                                           
            ('sch_pselprogenr_hi_m', 'el_program', 'hispanic', 'male'),
            ('sch_pselprogenr_hi_f', 'el_program', 'hispanic', 'female'),           
            ('sch_pselprogenr_am_m', 'el_program', 'american_indian', 'male'),
            ('sch_pselprogenr_am_f', 'el_program', 'american_indian', 'female'),    
            ('sch_pselprogenr_as_m', 'el_program', 'asian', 'male'),                
            ('sch_pselprogenr_as_f', 'el_program', 'asian', 'female'),
            ('sch_pselprogenr_hp_m', 'el_program', 'hawaiian_pacific_islander', 'male'),                                    
            ('sch_pselprogenr_hp_f', 'el_program', 'hawaiian_pacific_islander', 'female'),      
            ('sch_pselprogenr_bl_m', 'el_program', 'black', 'male'),                
            ('sch_pselprogenr_bl_f', 'el_program', 'black', 'female'),
            ('sch_pselprogenr_wh_m', 'el_program', 'white', 'male'),                
            ('sch_pselprogenr_wh_f', 'el_program', 'white', 'female'),
            ('sch_pselprogenr_tr_m', 'el_program', 'two_or_more_races', 'male'),    
            ('sch_pselprogenr_tr_f', 'el_program', 'two_or_more_races', 'female'),
            ('tot_pselprogenr_m', 'el_program', 'total', 'male'),                   
            ('tot_pselprogenr_f', 'el_program', 'total', 'female'),
            -- IDEA                                                                 
            ('sch_psideaenr_hi_m', 'idea', 'hispanic', 'male'),
            ('sch_psideaenr_hi_f', 'idea', 'hispanic', 'female'),                   
            ('sch_psideaenr_am_m', 'idea', 'american_indian', 'male'),
            ('sch_psideaenr_am_f', 'idea', 'american_indian', 'female'),            
            ('sch_psideaenr_as_m', 'idea', 'asian', 'male'),                        
            ('sch_psideaenr_as_f', 'idea', 'asian', 'female'),
            ('sch_psideaenr_hp_m', 'idea', 'hawaiian_pacific_islander', 'male'),    
            ('sch_psideaenr_hp_f', 'idea', 'hawaiian_pacific_islander', 'female'),  
            ('sch_psideaenr_bl_m', 'idea', 'black', 'male'),
            ('sch_psideaenr_bl_f', 'idea', 'black', 'female'),                      
            ('sch_psideaenr_wh_m', 'idea', 'white', 'male'),
            ('sch_psideaenr_wh_f', 'idea', 'white', 'female'),                      
            ('sch_psideaenr_tr_m', 'idea', 'two_or_more_races', 'male'),
            ('sch_psideaenr_tr_f', 'idea', 'two_or_more_races', 'female'),          
            ('sch_psideaenr_el_m', 'idea', 'english_learner', 'male'),
            ('sch_psideaenr_el_f', 'idea', 'english_learner', 'female'),            
            ('tot_psideaenr_m', 'idea', 'total', 'male'),
            ('tot_psideaenr_f', 'idea', 'total', 'female'),                         
            -- Section 504                  
            ('sch_ps504enr_hi_m', 'section_504', 'hispanic', 'male'),               
            ('sch_ps504enr_hi_f', 'section_504', 'hispanic', 'female'),
            ('sch_ps504enr_am_m', 'section_504', 'american_indian', 'male'),        
            ('sch_ps504enr_am_f', 'section_504', 'american_indian', 'female'),
            ('sch_ps504enr_as_m', 'section_504', 'asian', 'male'),                  
            ('sch_ps504enr_as_f', 'section_504', 'asian', 'female'),                
            ('sch_ps504enr_hp_m', 'section_504', 'hawaiian_pacific_islander', 'male'),                                                                            
            ('sch_ps504enr_hp_f', 'section_504', 'hawaiian_pacific_islander', 'female'),                                                                          
            ('sch_ps504enr_bl_m', 'section_504', 'black', 'male'),                  
            ('sch_ps504enr_bl_f', 'section_504', 'black', 'female'),
            ('sch_ps504enr_wh_m', 'section_504', 'white', 'male'),                  
            ('sch_ps504enr_wh_f', 'section_504', 'white', 'female'),
            ('sch_ps504enr_tr_m', 'section_504', 'two_or_more_races', 'male'),      
            ('sch_ps504enr_tr_f', 'section_504', 'two_or_more_races', 'female'),
            ('sch_ps504enr_el_m', 'section_504', 'english_learner', 'male'),        
            ('sch_ps504enr_el_f', 'section_504', 'english_learner', 'female'),
            ('tot_ps504enr_m', 'section_504', 'total', 'male'),                     
            ('tot_ps504enr_f', 'section_504', 'total', 'female')
    ) as m(col_name, program, subgroup, sex)                                        
  )                                           
                                                                                      
  select * from unpivoted