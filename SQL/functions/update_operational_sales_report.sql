-- DROP FUNCTION public.update_operational_sales_report();
CREATE OR REPLACE FUNCTION public.update_operational_sales_report()
 RETURNS integer
 LANGUAGE plpgsql
AS $function$
DECLARE
    rows_processed INTEGER := 0;
BEGIN
    -- Начало транзакции
    BEGIN
        -- Вставка данных в таблицу operational_sales_report
        INSERT INTO operational_sales_report (realization_date, property_type, income, deals_count)
        SELECT 
            ata.realization_date,
            COALESCE(
                CASE
                    WHEN d.deal_type = 'От застройщика' AND d.developer = 'Риелторское агентство Города' THEN 'Загородная новостройка'
                    WHEN d.deal_type = 'От застройщика' AND (d.developer <> 'Риелторское агентство Города' OR d.developer IS NULL) 
                         AND (b.object_type IS NULL OR b.object_type <> 'Коммерция') THEN 'Новостройка'
                    WHEN d.deal_type = 'От застройщика' AND (d.developer <> 'Риелторское агентство Города' OR d.developer IS NULL) 
                         AND b.object_type = 'Коммерция' THEN 'Коммерция'
                    WHEN o.land_status = 'Промназначения' THEN 'Коммерция'
                    ELSE o.property_type
                END, 
                d.object_type
            ) AS property_type,
            SUM(ata.income) AS income,
            COUNT(ata.id) AS deals_count
        FROM "access-to-accounting" ata
        LEFT JOIN deal d ON ata.deal_id = d.id
        LEFT JOIN objects o ON d.object_id = o.id
        LEFT JOIN users u ON ata.responsible_id = u.id
        LEFT JOIN bid b ON b.id = d.bid_id
        WHERE u.company::text = 'Города Новосибирск'::text 
          AND (ata.contract_name NOT IN ('Договор аренды', 'Эксклюзивный договор аренды помещения') OR ata.contract_name IS NULL)
          AND ata.status = 'Проведен'
        GROUP BY 
            ata.realization_date,
            COALESCE(
                CASE
                    WHEN d.deal_type = 'От застройщика' AND d.developer = 'Риелторское агентство Города' THEN 'Загородная новостройка'
                    WHEN d.deal_type = 'От застройщика' AND (d.developer <> 'Риелторское агентство Города' OR d.developer IS NULL) 
                         AND (b.object_type IS NULL OR b.object_type <> 'Коммерция') THEN 'Новостройка'
                    WHEN d.deal_type = 'От застройщика' AND (d.developer <> 'Риелторское агентство Города' OR d.developer IS NULL) 
                         AND b.object_type = 'Коммерция' THEN 'Коммерция'
                    WHEN o.land_status = 'Промназначения' THEN 'Коммерция'
                    ELSE o.property_type
                END, 
                d.object_type
            )
        ON CONFLICT (realization_date, property_type) 
        DO UPDATE SET
            income = EXCLUDED.income,
            deals_count = EXCLUDED.deals_count;

END;
$function$
;
