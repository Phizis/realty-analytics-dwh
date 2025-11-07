-- DROP FUNCTION public.update_hr_employee_analytics();
CREATE OR REPLACE FUNCTION public.update_hr_employee_analytics()
 RETURNS void
 LANGUAGE plpgsql
AS $function$
BEGIN
    WITH source_data AS (
        select 
    id,
    full_name ,
    company,
    office,
    department ,
    position,
    birth_date::date ,
    EXTRACT(YEAR FROM AGE(NOW(), birth_date::date))::INTEGER AS age,
    gender,
    coalesce(career_start , start_date ) as career_start,
    ROUND((date_part('year', now()) - date_part('year', coalesce(career_start, start_date))) * 12::double precision + (date_part('month', now()) - date_part('month', coalesce(career_start, start_date))))::integer AS experience,
    is_active ,
    case 
    	when is_active = 'true' then null
    	else deactivation_date
    end as deactivation_date    
    from users
    where company ilike 'Города%'
),
final_data AS (
    select 
sd.*,
CASE
        WHEN age BETWEEN 18 AND 25 THEN '18–25'
        WHEN age BETWEEN 26 AND 35 THEN '26–35'
        WHEN age BETWEEN 36 AND 45 THEN '36–45'
        WHEN age BETWEEN 46 AND 55 THEN '46–55'
        WHEN age >= 56 THEN '56+'
        ELSE 'Не указан / ошибка'
    END AS age_group,
CASE
        WHEN experience BETWEEN 0 AND 6 THEN '0-6'
        WHEN experience BETWEEN 7 AND 12 THEN '07-12'
        WHEN experience BETWEEN 13 AND 24 THEN '13-24'
        WHEN experience >= 25 THEN '25+'
    END AS exp_group,
CASE
        WHEN experience >=3 then true
        else false 
    END AS exp3plus,
CASE
        WHEN experience >=7 then TRUE    
        else false
    END AS exp7plus,
CASE
	WHEN birth_date IS NULL THEN 9999
	ELSE
	    (
	    birth_date
	    + ((EXTRACT(YEAR FROM CURRENT_DATE) - EXTRACT(YEAR FROM birth_date)) * INTERVAL '1 year')::INTERVAL
	    + CASE
	        WHEN (birth_date + ((EXTRACT(YEAR FROM CURRENT_DATE) - EXTRACT(YEAR FROM birth_date)) * INTERVAL '1 year'))::DATE < CURRENT_DATE
	        THEN INTERVAL '1 year'
	        ELSE INTERVAL '0 days'
	          END
	        )::DATE - CURRENT_DATE
END AS days_to_birthday
from source_data sd
)
INSERT INTO hr_employee_analytics (
    id,
    full_name,
    company,
    office,
    department,
    position,
    birth_date,
    age,
    gender,
    career_start,
    experience,
    is_active,
    deactivation_date,
    age_group,
    exp_group,
    exp3plus,
    exp7plus,
    days_to_birthday
)
SELECT 
    id,
    full_name,
    company,
    office,
    department,
    position,
    birth_date,
    age,
    gender,
    career_start,
    experience,
    is_active,
    deactivation_date,
    age_group,
    exp_group,
    exp3plus,
    exp7plus,
    days_to_birthday
FROM final_data
ON CONFLICT (id)
DO UPDATE SET
    full_name = EXCLUDED.full_name,
    company = EXCLUDED.company,
    office = EXCLUDED.office,
    department = EXCLUDED.department,
    position = EXCLUDED.position,
    birth_date = EXCLUDED.birth_date,
    age = EXCLUDED.age,
    gender = EXCLUDED.gender,
    career_start = EXCLUDED.career_start,
    experience = EXCLUDED.experience,
    is_active = EXCLUDED.is_active,
    deactivation_date = EXCLUDED.deactivation_date,
    age_group = EXCLUDED.age_group,
    exp_group = EXCLUDED.exp_group,
    exp3plus = EXCLUDED.exp3plus,
    exp7plus = EXCLUDED.exp7plus,
	days_to_birthday = EXCLUDED.days_to_birthday;
END;
$function$
;
