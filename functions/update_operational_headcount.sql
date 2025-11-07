-- DROP FUNCTION public.update_operational_headcount();
CREATE OR REPLACE FUNCTION public.update_operational_headcount()
 RETURNS integer
 LANGUAGE plpgsql
AS $function$
DECLARE
    rows_processed INTEGER := 0;
BEGIN
    -- Начало транзакции
    BEGIN
        WITH source_data AS (
select 
now()::date-1 as date,
users_count,
exp
from
(
-- 0-6
select count(id) as users_count,
((date_part('year'::text, now()) - date_part('year'::text, coalesce(career_start,start_date))) * 12 + (date_part('month'::text, now()) - date_part('month'::text, coalesce(career_start,start_date))))::varchar as exp
from users
where company = 'Города Новосибирск'
and is_active = 'true'
and office <> 'Аренда'
and (department ilike 'Команда%'
or department in ('Отдел коммерческой недвижимости'))
and "position" in ('Эксперт по недвижимости', 'Специалист по недвижимости', 'Специалист отдела коммерческой недвижимости')
and 
(date_part('year'::text, now()) - date_part('year'::text, coalesce(career_start,start_date))) * 12::double precision + (date_part('month'::text, now()) - date_part('month'::text, coalesce(career_start,start_date)))::integer < 7
group by ((date_part('year'::text, now()) - date_part('year'::text, coalesce(career_start,start_date))) * 12 + (date_part('month'::text, now()) - date_part('month'::text, coalesce(career_start,start_date))))::varchar
union
-- 24+
select 
sum(id) as users_count,
'>24' as exp
from(
select count(id) id ,
(date_part('year'::text, now()) - date_part('year'::text, coalesce(career_start,start_date))) * 12::double precision + (date_part('month'::text, now()) - date_part('month'::text, coalesce(career_start,start_date)))::integer as exp
from users
where company = 'Города Новосибирск'
and is_active = 'true'
and office <> 'Аренда'
and (department ilike 'Команда%'
or department in ('Отдел коммерческой недвижимости'))
and "position" in ('Эксперт по недвижимости', 'Специалист по недвижимости', 'Специалист отдела коммерческой недвижимости')
and (date_part('year'::text, now()) - date_part('year'::text, coalesce(career_start,start_date))) * 12::double precision + (date_part('month'::text, now()) - date_part('month'::text, coalesce(career_start,start_date)))::integer 
>24
group by (date_part('year'::text, now()) - date_part('year'::text, coalesce(career_start,start_date))) * 12::double precision + (date_part('month'::text, now()) - date_part('month'::text, coalesce(career_start,start_date)))::integer
)base
union
-- 7-12
select 
sum(id) as users_count,
'7-12' as exp
from(
select count(id) id ,
(date_part('year'::text, now()) - date_part('year'::text, coalesce(career_start,start_date))) * 12::double precision + (date_part('month'::text, now()) - date_part('month'::text, coalesce(career_start,start_date)))::integer as exp
from users
where company = 'Города Новосибирск'
and is_active = 'true'
and office <> 'Аренда'
and (department ilike 'Команда%'
or department in ('Отдел коммерческой недвижимости'))
and "position" in ('Эксперт по недвижимости', 'Специалист по недвижимости', 'Специалист отдела коммерческой недвижимости')
and (date_part('year'::text, now()) - date_part('year'::text, coalesce(career_start,start_date))) * 12::double precision + (date_part('month'::text, now()) - date_part('month'::text, coalesce(career_start,start_date)))::integer 
between 7 and 12
group by (date_part('year'::text, now()) - date_part('year'::text, coalesce(career_start,start_date))) * 12::double precision + (date_part('month'::text, now()) - date_part('month'::text, coalesce(career_start,start_date)))::integer
)base
union
-- 13-24
select 
sum(id) as users_count,
'13-24' as exp
from(
select count(id) id ,
(date_part('year'::text, now()) - date_part('year'::text, coalesce(career_start,start_date))) * 12::double precision + (date_part('month'::text, now()) - date_part('month'::text, coalesce(career_start,start_date)))::integer as exp
from users
where company = 'Города Новосибирск'
and is_active = 'true'
and office <> 'Аренда'
and (department ilike 'Команда%'
or department in ('Отдел коммерческой недвижимости'))
and "position" in ('Эксперт по недвижимости', 'Специалист по недвижимости', 'Специалист отдела коммерческой недвижимости')
and (date_part('year'::text, now()) - date_part('year'::text, coalesce(career_start,start_date))) * 12::double precision + (date_part('month'::text, now()) - date_part('month'::text, coalesce(career_start,start_date)))::integer 
between 13 and 24
group by (date_part('year'::text, now()) - date_part('year'::text, coalesce(career_start,start_date))) * 12::double precision + (date_part('month'::text, now()) - date_part('month'::text, coalesce(career_start,start_date)))::integer
)base
union
-- Менеджеры ОП
select
count(id) as users_count,
'Менеджеры ОП' as exp
from users
where company = 'Города Новосибирск'
and is_active = 'true'
and office <> 'Аренда'
and (department ilike 'Команда%'
or department in ('Отдел коммерческой недвижимости'))
and "position" = 'Менеджер отдела продаж'
union
-- Помощники
select
count(id) as users_count,
'Помощники ОП' as exp
from users
where company = 'Города Новосибирск'
and is_active = 'true'
and office <> 'Аренда'
and (department ilike 'Команда%'
or department in ('Отдел коммерческой недвижимости'))
and "position" in ('Помощник специалиста', 'Помощник менеджера')
union
-- Декрет
select
count(id) as users_count,
'Декрет' as exp
from users
where company = 'Города Новосибирск'
and is_active = 'true'
and office <> 'Аренда'
and "position" = 'Старший агент по недвижимости'
union
-- Группа 0
select
count(id) as users_count,
'Группа 0' as exp
from users
where company = 'Города Новосибирск'
and is_active = 'true'
and office <> 'Аренда'
and "position" = 'Агент по недвижимости'
union
-- АУП ОП
select 
count(id) as users_count,
'АУП ОП' as exp
from
(select * 
from users
where company = 'Города Новосибирск' and is_active = 'true' and department not ilike 'Команда%'
and department not in ('Отдел коммерческой недвижимости', 'Декрет', 'Группа 0', 'Технические аккаунты')
and office <> 'Аренда'
and position not in ('Помощник специалиста', 'Помощник менеджера')
)base
union
-- Менеджеры ОА
select
count(id) as users_count,
'Менеджеры ОА' as exp
from users
where company = 'Города Новосибирск' and is_active = 'true' 
and office = 'Аренда'
and "position" = 'Менеджер отдела аренды'
union
-- Помощники ОА
select
count(id) as users_count,
'Помощники ОА' as exp
from users
where company = 'Города Новосибирск' and is_active = 'true' 
and office = 'Аренда'
and "position" = 'Помощник специалиста'
union
-- АУП ОА
select
count(id) as users_count,
'АУП ОА' as exp
from users
where company = 'Города Новосибирск' and is_active = 'true' 
and office = 'Аренда'
and "position" in ('Администратор', 'Руководитель')
union
-- >6 Аренда
select
count(id) as users_count,
'>6' as exp
from users
where company = 'Города Новосибирск' and is_active = 'true' 
and office = 'Аренда'
and "position" = 'Специалист по аренде недвижимости'
and (date_part('year'::text, now()) - date_part('year'::text, coalesce(career_start,start_date))) * 12::double precision + (date_part('month'::text, now()) - date_part('month'::text, coalesce(career_start,start_date)))::integer
> 6
union
-- 0-3 Аренда
select
count(id) as users_count,
'0-3' as exp
from users
where company = 'Города Новосибирск' and is_active = 'true' 
and office = 'Аренда'
and "position" = 'Специалист по аренде недвижимости'
and (date_part('year'::text, now()) - date_part('year'::text, coalesce(career_start,start_date))) * 12::double precision + (date_part('month'::text, now()) - date_part('month'::text, coalesce(career_start,start_date)))::integer
between 0 and 3
union
-- 4-6 Аренда
select
count(id) as users_count,
'4-6' as exp
from users
where company = 'Города Новосибирск' and is_active = 'true' 
and office = 'Аренда'
and "position" = 'Специалист по аренде недвижимости'
and (date_part('year'::text, now()) - date_part('year'::text, coalesce(career_start,start_date))) * 12::double precision + (date_part('month'::text, now()) - date_part('month'::text, coalesce(career_start,start_date)))::integer
between 4 and 6) base)
        INSERT INTO operational_headcount_report (
            date, users_count, exp
        )
        SELECT 
            date, users_count, exp
        FROM source_data
        ON CONFLICT (date, exp) 
        DO UPDATE SET
            users_count = EXCLUDED.users_count;
       
END;
$function$
;
