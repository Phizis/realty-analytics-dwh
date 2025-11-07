-- DROP FUNCTION public.update_daily_personnel();
CREATE OR REPLACE FUNCTION public.update_daily_personnel()
 RETURNS void
 LANGUAGE plpgsql
AS $function$
BEGIN

-- Исходный запрос без изменений:
INSERT INTO daily_personnel (
    date,
    id,
    full_name,
    company,
    department,
    "position",
    career_start,
    office,
    is_active,
    deactivation_date,
    deactivation_period,
    experience,
	gender,
	birth_date,
	age
)
SELECT 
    c.date,
    id,
    full_name,
    company,
    department,
    "position",
    coalesce(career_start, start_date) AS career_start,
    office,
    is_active,
    deactivation_date,
    coalesce(
        (date_part('year', now()) - date_part('year', deactivation_date)) * 12 
        + (date_part('month', now()) - date_part('month', deactivation_date)), 
        0
    ) AS deactivation_period,
        (date_part('year'::text, c.date) - date_part('year'::text, coalesce(career_start, start_date))) * 12::double precision + (date_part('month'::text, c.date) - date_part('month'::text, coalesce(career_start, start_date))) AS experience,
        u.gender,        
        u.birth_date::DATE,
        ROUND(((date_part('year'::text, c.date) - date_part('year'::text, u.birth_date::date)) * 12 + (date_part('month'::text, c.date) - date_part('month'::text, u.birth_date::date)))::NUMERIC/12, 2) AS age
FROM 
    users u
CROSS JOIN 
    calendar c
WHERE 
    c.date = CURRENT_DATE -- Используем текущую дату
    AND company like 'Города%'
    AND ( u.is_active = 'true' or coalesce(
        (date_part('year', now()) - date_part('year', deactivation_date)) * 12 
        + (date_part('month', now()) - date_part('month', deactivation_date)), 
        0
    ) < 5 )
ON CONFLICT (date, id) DO UPDATE SET
    full_name = EXCLUDED.full_name,
    company = EXCLUDED.company,
    department = EXCLUDED.department,
    "position" = EXCLUDED."position",
    career_start = EXCLUDED.career_start,
    office = EXCLUDED.office,
    is_active = EXCLUDED.is_active,
    deactivation_date = EXCLUDED.deactivation_date,
    deactivation_period = EXCLUDED.deactivation_period,
    experience = EXCLUDED.experience,
	gender = EXCLUDED.gender,
	birth_date = EXCLUDED.birth_date,
	age = EXCLUDED.age;

END;
$function$
;
