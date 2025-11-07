-- DROP FUNCTION public.refresh_dm_sales_staff_deals();
CREATE OR REPLACE FUNCTION public.refresh_dm_sales_staff_deals()
 RETURNS void
 LANGUAGE plpgsql
AS $function$
BEGIN

    -- 1. Вставка/обновление строк СО сделками
    INSERT INTO dm_sales_staff_deals (
        year, month, user_id, full_name, department, "position", is_active,
        experience, company, realization_date, income, ata_id,
        property_type, deal_category, is_first_valid_deal
    )
    SELECT 
        year, month, user_id, full_name, department, "position", is_active,
        experience, company, realization_date, income, ata_id,
        property_type, deal_category, is_first_valid_deal
    FROM (
        WITH 
        -- 1. Сотрудники, зафиксированные в daily_personnel на 5-е число (опыт > 0)
        staff_on_5th AS (
            SELECT 
                dp.id AS user_id,
                dp.full_name,
                dp.department,
                dp."position",
                dp.is_active,
                dp.experience,
                dp.company,
                date_part('year', dp.date)::int AS year,
                date_part('month', dp.date)::int AS month
            FROM daily_personnel dp
            WHERE 
                date_part('day', dp.date) = 5
                AND dp.office <> 'Аренда'
                AND dp.company = 'Города Новосибирск'
                AND dp.experience > 0
        ),
        -- 2. Новички: пришли в этом месяце (опыт = 0 на момент сделки) и есть сделка
        newcomers AS (
            SELECT DISTINCT
                u.id AS user_id,
                u.full_name,
                u.department,
                u."position",
                u.is_active,
                0 AS experience,
                u.company,
                date_part('year', ata.realization_date)::int AS year,
                date_part('month', ata.realization_date)::int AS month
            FROM "access-to-accounting" ata
            JOIN users u 
                ON ata.responsible_id = u.id
            WHERE 
                ata.company = 'Города Новосибирск'
                AND ata.office <> 'Аренда'
                AND ata.realization_date IS NOT NULL
                AND ata.status = 'Проведен'        
                AND (
                    (date_part('year', ata.realization_date) - date_part('year', COALESCE(u.career_start, u.start_date))) * 12
                    + (date_part('month', ata.realization_date) - date_part('month', COALESCE(u.career_start, u.start_date)))
                ) = 0
        ),
        -- 3. Объединяем всех сотрудников отдела продаж по месяцам (без дублей)
        all_sales_staff AS (
            SELECT * FROM staff_on_5th
            UNION
            SELECT * FROM newcomers
        ),
        -- 4. Валидные сделки (не арендные, проведённые) с property_type
        valid_deals AS (
            SELECT DISTINCT
                ata.realization_date,
                date_part('year', ata.realization_date)::int AS year,
                date_part('month', ata.realization_date)::int AS month,
                ata.responsible_id AS user_id,
                ata.income,
                ata.id AS ata_id,
                ata.contract_name,
                ata.status,
                COALESCE(
                    CASE
                        WHEN d.deal_type = 'От застройщика' AND d.developer = 'Риелторское агентство Города' THEN 'Загородная новостройка'
                        WHEN d.deal_type = 'От застройщика' 
                             AND (d.developer <> 'Риелторское агентство Города' OR d.developer IS NULL) 
                             AND (b.object_type IS NULL OR b.object_type <> 'Коммерция') THEN 'Новостройка'
                        WHEN d.deal_type = 'От застройщика' 
                             AND (d.developer <> 'Риелторское агентство Города' OR d.developer IS NULL) 
                             AND b.object_type = 'Коммерция' THEN 'Коммерция'
                        WHEN o.land_status = 'Промназначения' THEN 'Коммерция'
                        ELSE o.property_type
                    END, 
                    d.object_type
                ) AS property_type
            FROM "access-to-accounting" ata
            LEFT JOIN deal d ON ata.deal_id = d.id
            LEFT JOIN objects o ON d.object_id = o.id
            LEFT JOIN bid b ON b.id = d.bid_id
            WHERE 
                ata.company = 'Города Новосибирск'
                AND ata.office <> 'Аренда'
                AND ata.status = 'Проведен'
                AND ata.realization_date IS NOT NULL
        ),
        -- 5. Объединяем: все сотрудники + их валидные сделки (LEFT JOIN!)
        staff_with_deals AS (
            SELECT
                ss.year,
                ss.month,
                ss.user_id,
                ss.full_name,
                ss.department,
                ss."position",
                ss.is_active,
                ss.experience,
                ss.company,
                vd.realization_date,
                vd.income,
                vd.ata_id,
                coalesce(vd.property_type, vd.contract_name) as property_type
            FROM all_sales_staff ss
            LEFT JOIN valid_deals vd 
                ON ss.user_id = vd.user_id 
                AND ss.year = vd.year 
                AND ss.month = vd.month
        ),
        -- 6. Помечаем первую валидную сделку в месяце (для СДС)
        final AS (
            SELECT *,
                CASE 
                    WHEN realization_date IS NOT NULL 
                         AND property_type IS NOT NULL
                         AND property_type NOT ILIKE '%аренд%'
                         AND ROW_NUMBER() OVER (
                             PARTITION BY year, month, user_id 
                             ORDER BY realization_date, ata_id
                         ) = 1 
                    THEN 1 
                    ELSE 0 
                END AS is_first_valid_deal
            FROM staff_with_deals
        )
        SELECT 
            year,
            month,
            user_id,
            full_name,
            department,
            "position",
            is_active,
            experience,
            company,
            realization_date,
            income,
            ata_id,
            property_type,
            CASE
                WHEN property_type IS NULL THEN NULL
                WHEN property_type ILIKE '%аренд%' THEN 'Аренда'
                ELSE 'Продажа'
            END AS deal_category,
            is_first_valid_deal
        FROM final
        WHERE ata_id IS NOT NULL
    ) AS source_data
    ON CONFLICT (user_id, year, month, ata_id) 
    DO UPDATE SET
        full_name = EXCLUDED.full_name,
        department = EXCLUDED.department,
        "position" = EXCLUDED."position",
        is_active = EXCLUDED.is_active,
        experience = EXCLUDED.experience,
        company = EXCLUDED.company,
        realization_date = EXCLUDED.realization_date,
        income = EXCLUDED.income,
        property_type = EXCLUDED.property_type,
        deal_category = EXCLUDED.deal_category,
        is_first_valid_deal = EXCLUDED.is_first_valid_deal;

    -- 2. Вставка строк БЕЗ сделок (только если ещё не существует)
    INSERT INTO dm_sales_staff_deals (
        year, month, user_id, full_name, department, "position", is_active,
        experience, company, realization_date, income, ata_id,
        property_type, deal_category, is_first_valid_deal
    )
    SELECT 
        year, month, user_id, full_name, department, "position", is_active,
        experience, company, realization_date, income, ata_id,
        property_type, deal_category, is_first_valid_deal
    FROM (
        WITH 
        -- 1. Сотрудники, зафиксированные в daily_personnel на 5-е число (опыт > 0)
        staff_on_5th AS (
            SELECT 
                dp.id AS user_id,
                dp.full_name,
                dp.department,
                dp."position",
                dp.is_active,
                dp.experience,
                dp.company,
                date_part('year', dp.date)::int AS year,
                date_part('month', dp.date)::int AS month
            FROM daily_personnel dp
            WHERE 
                date_part('day', dp.date) = 5
                AND dp.office <> 'Аренда'
                AND dp.company = 'Города Новосибирск'
                AND dp.experience > 0
        ),
        -- 2. Новички: пришли в этом месяце (опыт = 0 на момент сделки) и есть сделка
        newcomers AS (
            SELECT DISTINCT
                u.id AS user_id,
                u.full_name,
                u.department,
                u."position",
                u.is_active,
                0 AS experience,
                u.company,
                date_part('year', ata.realization_date)::int AS year,
                date_part('month', ata.realization_date)::int AS month
            FROM "access-to-accounting" ata
            JOIN users u 
                ON ata.responsible_id = u.id
            WHERE 
                ata.company = 'Города Новосибирск'
                AND ata.office <> 'Аренда'
                AND ata.realization_date IS NOT NULL
                AND ata.status = 'Проведен'        
                AND (
                    (date_part('year', ata.realization_date) - date_part('year', COALESCE(u.career_start, u.start_date))) * 12
                    + (date_part('month', ata.realization_date) - date_part('month', COALESCE(u.career_start, u.start_date)))
                ) = 0
        ),
        -- 3. Объединяем всех сотрудников отдела продаж по месяцам (без дублей)
        all_sales_staff AS (
            SELECT * FROM staff_on_5th
            UNION
            SELECT * FROM newcomers
        ),
        -- 4. Валидные сделки (не арендные, проведённые) с property_type
        valid_deals AS (
            SELECT DISTINCT
                ata.realization_date,
                date_part('year', ata.realization_date)::int AS year,
                date_part('month', ata.realization_date)::int AS month,
                ata.responsible_id AS user_id,
                ata.income,
                ata.id AS ata_id,
                ata.contract_name,
                ata.status,
                COALESCE(
                    CASE
                        WHEN d.deal_type = 'От застройщика' AND d.developer = 'Риелторское агентство Города' THEN 'Загородная новостройка'
                        WHEN d.deal_type = 'От застройщика' 
                             AND (d.developer <> 'Риелторское агентство Города' OR d.developer IS NULL) 
                             AND (b.object_type IS NULL OR b.object_type <> 'Коммерция') THEN 'Новостройка'
                        WHEN d.deal_type = 'От застройщика' 
                             AND (d.developer <> 'Риелторское агентство Города' OR d.developer IS NULL) 
                             AND b.object_type = 'Коммерция' THEN 'Коммерция'
                        WHEN o.land_status = 'Промназначения' THEN 'Коммерция'
                        ELSE o.property_type
                    END, 
                    d.object_type
                ) AS property_type
            FROM "access-to-accounting" ata
            LEFT JOIN deal d ON ata.deal_id = d.id
            LEFT JOIN objects o ON d.object_id = o.id
            LEFT JOIN bid b ON b.id = d.bid_id
            WHERE 
                ata.company = 'Города Новосибирск'
                AND ata.office <> 'Аренда'
                AND ata.status = 'Проведен'
                AND ata.realization_date IS NOT NULL
        ),
        -- 5. Объединяем: все сотрудники + их валидные сделки (LEFT JOIN!)
        staff_with_deals AS (
            SELECT
                ss.year,
                ss.month,
                ss.user_id,
                ss.full_name,
                ss.department,
                ss."position",
                ss.is_active,
                ss.experience,
                ss.company,
                vd.realization_date,
                vd.income,
                vd.ata_id,
                coalesce(vd.property_type, vd.contract_name) as property_type
            FROM all_sales_staff ss
            LEFT JOIN valid_deals vd 
                ON ss.user_id = vd.user_id 
                AND ss.year = vd.year 
                AND ss.month = vd.month
        ),
        -- 6. Помечаем первую валидную сделку в месяце (для СДС)
        final AS (
            SELECT *,
                CASE 
                    WHEN realization_date IS NOT NULL 
                         AND property_type IS NOT NULL
                         AND property_type NOT ILIKE '%аренд%'
                         AND ROW_NUMBER() OVER (
                             PARTITION BY year, month, user_id 
                             ORDER BY realization_date, ata_id
                         ) = 1 
                    THEN 1 
                    ELSE 0 
                END AS is_first_valid_deal
            FROM staff_with_deals
        )
        SELECT 
            year,
            month,
            user_id,
            full_name,
            department,
            "position",
            is_active,
            experience,
            company,
            realization_date,
            income,
            ata_id,
            property_type,
            CASE
                WHEN property_type IS NULL THEN NULL
                WHEN property_type ILIKE '%аренд%' THEN 'Аренда'
                ELSE 'Продажа'
            END AS deal_category,
            is_first_valid_deal
        FROM final
        WHERE ata_id IS NULL
    ) AS source_data
    WHERE NOT EXISTS (
        SELECT 1
        FROM dm_sales_staff_deals existing
        WHERE existing.user_id = source_data.user_id
          AND existing.year = source_data.year
          AND existing.month = source_data.month
          AND existing.ata_id IS NULL
    );

END;
$function$
;
