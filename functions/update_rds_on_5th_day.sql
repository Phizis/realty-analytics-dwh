-- DROP FUNCTION public.update_rds_on_5th_day();
CREATE OR REPLACE FUNCTION public.update_rds_on_5th_day()
 RETURNS integer
 LANGUAGE plpgsql
AS $function$
DECLARE
    rows_processed INTEGER := 0;
BEGIN
    -- Начало транзакции
    BEGIN
        -- Вставка данных в таблицу rds_on_5th_day
        INSERT INTO rds_on_5th_day (
            month,
            year,
            user_id,
            full_name,
            department,
            "position",
            is_active,
            experience,
            exp_category,
            status,
            income,
            deal
        )
        SELECT 
            date_part('month', dp.date)::integer AS month,
            date_part('year', dp.date)::integer AS year,
            dp.id AS user_id,
            dp.full_name,
            dp.department,
            dp."position",
            CASE
                WHEN dp.is_active = 'false' THEN 'Уволен'
                ELSE 'Активен'
            END AS is_active,        
            dp.experience::integer,
            CASE
                WHEN dp.experience > 24 THEN '24+'
                WHEN dp.experience > 12 AND dp.experience < 25 THEN '13-24'
                WHEN dp.experience > 6 AND dp.experience < 13 THEN '07-12'
                WHEN dp.experience > 2 AND dp.experience < 7 THEN '03-06'
                ELSE CONCAT('0', dp.experience::VARCHAR)
            END AS exp_category,        
            COALESCE(ata.status, 'Нет данных') AS status,
            COALESCE(SUM(CASE WHEN ata.status = 'Проведен' THEN ata.income ELSE 0 END), 0) AS income,
            SUM(
                CASE
                    WHEN ata.contract_name ILIKE '%аренд%' OR ata.status <> 'Проведен' OR ata.status IS NULL THEN 0
                    ELSE 1
                END
            ) AS deal
        FROM daily_personnel dp
        LEFT JOIN "access-to-accounting" ata
            ON dp.id = ata.responsible_id
            AND date_part('month', dp.date) = date_part('month', ata.realization_date)
            AND date_part('year', dp.date) = date_part('year', ata.realization_date)
        WHERE 
            date_part('day', dp.date) = 5
            AND dp.office <> 'Аренда'
            AND dp.experience > 0
        GROUP BY 
            date_part('month', dp.date),
            date_part('year', dp.date),
            dp.id,
            dp.full_name,
            dp.department,
            dp."position",
            dp.is_active,
            dp.experience,
            ata.status
        ON CONFLICT (month, year, user_id, status) DO UPDATE SET
            full_name = EXCLUDED.full_name,
            department = EXCLUDED.department,
            "position" = EXCLUDED."position",
            is_active = EXCLUDED.is_active,
            experience = EXCLUDED.experience,
            exp_category = EXCLUDED.exp_category,
            income = EXCLUDED.income,
            deal = EXCLUDED.deal;

END;
$function$
;
