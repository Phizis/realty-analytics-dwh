-- DROP FUNCTION public.update_traffic_light_1m();
CREATE OR REPLACE FUNCTION public.update_traffic_light_1m()
 RETURNS void
 LANGUAGE plpgsql
AS $function$
BEGIN
    WITH source_data AS (
    SELECT 
        date_part('month', dp.date) AS month,
        date_part('year', dp.date) AS year,
        dp.id AS user_id,
        dp.full_name,
        dp.department,
        dp."position",
        dp.is_active,
        dp.experience,
        coalesce(ata.id,0) AS ata_id, -- Может быть NULL
        CASE
            WHEN ata.status = 'Проведен' THEN 'Проведен'
            ELSE 'Нет данных'
        END AS status,
        ata.realization_date,
        COALESCE(ata.contract_id, 1) AS contract_id,
        CASE
            WHEN ata.contract_name IS NULL AND ata.status = 'Проведен' THEN 'Новостройка'
            WHEN ata.contract_name IS NULL THEN 'Нет данных'
            ELSE ata.contract_name
        END AS contract_name,
        COALESCE(ata.deal_type, 'Не размечено') AS deal_type,
        CASE
            WHEN ata.status = 'Проведен' THEN SUM(ata.income)
            ELSE 0
        END AS income,
        CASE
            WHEN ata.status = 'Проведен' THEN SUM(ata.payment)
            ELSE 0
        END AS payment,
        CASE
            WHEN ata.contract_name ILIKE '%аренд%' OR ata.status <> 'Проведен' OR ata.status IS NULL THEN 0
            ELSE 1
        END AS deal,
        CASE 
            WHEN ROW_NUMBER() OVER (PARTITION BY date_part('month', dp.date), date_part('year', dp.date), dp.id ORDER BY dp.id) = 1 THEN 1
            ELSE 0
        END AS is_first_occurrence
    FROM daily_personnel dp
    LEFT JOIN "access-to-accounting" ata
        ON dp.id = ata.responsible_id
        AND date_part('month', dp.date) = date_part('month', ata.realization_date)
        AND date_part('year', dp.date) = date_part('year', ata.realization_date)
    WHERE 
        date_part('day', dp.date) = 5
        AND dp.office <> 'Аренда'
        and dp.experience > 0
    GROUP BY 
        date_part('month', dp.date),
        date_part('year', dp.date),
        dp.id,
        dp.full_name,
        dp.department,
        dp."position",
        dp.is_active,
        dp.experience,
        ata.id, -- Может быть NULL
        ata.realization_date,
        ata.status,
        ata.contract_id,
        ata.contract_name,
        ata.deal_type        
	union 	
SELECT 
	date_part('month', ata.realization_date) as month,
	date_part('year', ata.realization_date) as year,
	ata.responsible_id as user_id,
    ata.responsible_name as full_name,
    ata.team as department,
    u."position" ,
    u.is_active ,
    (date_part('year'::text, ata.realization_date) - date_part('year'::text, coalesce(career_start, start_date))) * 12::double precision + (date_part('month'::text, ata.realization_date) - date_part('month'::text, coalesce(career_start, start_date))) AS experience,
	coalesce(ata.id,0) as ata_id,
	CASE
            WHEN ata.status = 'Проведен' THEN 'Проведен'
            ELSE 'Нет данных'
        END AS status,
        ata.realization_date,
        COALESCE(ata.contract_id, 1) AS contract_id,
        CASE
            WHEN ata.contract_name IS NULL AND ata.status = 'Проведен' THEN 'Новостройка'
            WHEN ata.contract_name IS NULL THEN 'Нет данных'
            ELSE ata.contract_name
        END AS contract_name,
        COALESCE(ata.deal_type, 'Не размечено') AS deal_type,
        CASE
            WHEN ata.status = 'Проведен' THEN SUM(ata.income)
            ELSE 0
        END AS income,
        CASE
            WHEN ata.status = 'Проведен' THEN SUM(ata.payment)
            ELSE 0
        END AS payment,
        CASE
            WHEN ata.contract_name ILIKE '%аренд%' OR ata.status <> 'Проведен' OR ata.status IS NULL THEN 0
            ELSE 1
        END AS deal,
        CASE 
            WHEN ROW_NUMBER() OVER (ORDER BY ata.responsible_id) = 1 THEN 1
            ELSE 0
        END AS is_first_occurrence
   FROM "access-to-accounting" ata
   left join users u 
   on ata.responsible_id = u.id
  WHERE ata.company::text = 'Города Новосибирск'::text AND ata.office::text <> 'Аренда'::text
  and
  (date_part('year'::text, ata.realization_date) - date_part('year'::text, coalesce(career_start, start_date))) * 12::double precision + (date_part('month'::text, ata.realization_date) - date_part('month'::text, coalesce(career_start, start_date))) = 0
  GROUP BY 
        date_part('month', ata.realization_date),
		date_part('year', ata.realization_date),
		ata.responsible_id,
	    ata.responsible_name,
	    ata.team,
	    u."position" ,
	    u.is_active ,
	  	ata.id,
	  	career_start, start_date, ata.status, ata.realization_date, ata.contract_id, ata.contract_name, ata.deal_type	  	
	  	),
-- Разделение данных на две части
source_data_with_ata_id AS (
    SELECT * FROM source_data WHERE ata_id IS NOT NULL
)
-- UPSERT для записей с ata_id IS NOT NULL
INSERT INTO traffic_light_1m (
    month, year, user_id, full_name, department, "position", is_active, experience,
    ata_id, status, realization_date, contract_id, contract_name, deal_type,
    income, payment, deal, is_first_occurrence
)
SELECT 
    month, year, user_id, full_name, department, "position", is_active, experience,
    ata_id, status, realization_date, contract_id, contract_name, deal_type,
    income, payment, deal, is_first_occurrence
FROM source_data_with_ata_id
ON CONFLICT (month, year, user_id, ata_id) 
DO UPDATE SET
    full_name = EXCLUDED.full_name,
    department = EXCLUDED.department,
    "position" = EXCLUDED."position",
    is_active = EXCLUDED.is_active,
    experience = EXCLUDED.experience,
    status = EXCLUDED.status,
    realization_date = EXCLUDED.realization_date,
    contract_id = EXCLUDED.contract_id,
    contract_name = EXCLUDED.contract_name,
    deal_type = EXCLUDED.deal_type,
    income = EXCLUDED.income,
    payment = EXCLUDED.payment,
    deal = EXCLUDED.deal,
    is_first_occurrence = EXCLUDED.is_first_occurrence;
END;
$function$
;
