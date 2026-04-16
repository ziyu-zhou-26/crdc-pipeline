{{ config(materialized='table') }}
                                                                                                
select
    survey_year,                                                                              
    label,      
    vintage
from (
    values
        (2018, '2017-18', '2017-18'),
        (2021, '2020-21', '2020-21'),
        (2022, '2021-22', '2021-22')                                                          
) as t(survey_year, label, vintage)