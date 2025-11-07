-- DROP FUNCTION public.refresh_sales_staff_agg_income();
CREATE OR REPLACE FUNCTION public.refresh_sales_staff_agg_income()
 RETURNS void
 LANGUAGE plpgsql
AS $function$
BEGIN
    INSERT INTO sales_staff_agg_income (
        year, month, full_name, department, position, company, 
        property_type, deal_category, deal_count, deal_income, avg_income
    )
    SELECT 
        year, 
        month, 
        full_name, 
        department, 
        position, 
        company, 
        property_type, 
        deal_category, 
        COUNT(full_name) AS deal_count,
        SUM(income) AS deal_income,
        ROUND(SUM(income) / COUNT(full_name)) AS avg_income
    FROM dm_sales_staff_deals
    WHERE property_type IS NOT NULL
      AND deal_category IS NOT NULL
    GROUP BY 
        year, month, full_name, department, position, 
        company, property_type, deal_category

    ON CONFLICT (year, month, full_name, property_type, department, position)
    DO UPDATE SET
        deal_category = EXCLUDED.deal_category,
        deal_count = EXCLUDED.deal_count,
        deal_income = EXCLUDED.deal_income,
        avg_income = EXCLUDED.avg_income;
END;
$function$
;
