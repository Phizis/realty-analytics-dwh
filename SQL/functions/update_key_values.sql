-- DROP FUNCTION public.update_key_values();
CREATE OR REPLACE FUNCTION public.update_key_values()
 RETURNS integer
 LANGUAGE plpgsql
AS $function$
DECLARE
    rows_processed INTEGER := 0;
BEGIN
    WITH source_data AS (
        SELECT 
            date_part('month'::text, dp.date) AS month,
            date_part('year'::text, dp.date) AS year,
            dp.id, 
            dp.full_name, 
            dp.department, 
            dp.position, 
            (date_part('year'::text, dp.date) - date_part('year'::text, career_start)) * 12::double precision + (date_part('month'::text, dp.date) - date_part('month'::text, career_start)) AS experience,
            COALESCE(cdd.deals, 0) as nvstr,
            COALESCE(ec.count, 0) as exclusives,
            COALESCE(dc.deals, 0) as deals,
            COALESCE(mcl.leads, 0) as mortgage_leads,
            COALESCE(mcc.leads, 0) as mortgage_consult,
            COALESCE(ao.objects_count, 0) as all_objects,
            COALESCE(mo.objects_count, 0) as new_objects,
            COALESCE(cco.cost_change_count, 0) as cost_change_count,
            COALESCE(bbc.bids, 0) as bids,
            COALESCE(atc.ads_count, 0) as ads_count,
            COALESCE(atc.avito_count, 0) as avito_count,
            COALESCE(atc.cian_count, 0) as cian_count,
            COALESCE(atc.yandex_count, 0) as yandex_count,
            COALESCE(atc.domclick_count, 0) as domclick_count,
            COALESCE(cdd.deals, 0) + COALESCE(ec.count, 0) + COALESCE(dc.deals, 0)
                + COALESCE(mcl.leads, 0) + COALESCE(mcc.leads, 0) + COALESCE(ao.objects_count, 0)
                + COALESCE(mo.objects_count, 0) + COALESCE(cco.cost_change_count, 0)
                + COALESCE(bbc.bids, 0) + COALESCE(atc.ads_count, 0) as all_sum,
            COALESCE(atc.avician_count, 0) as avician
        FROM daily_personnel dp
            LEFT JOIN count_developer_deals cdd ON dp.id = cdd.responsible_id AND date_part('month'::text, dp.date) = cdd."month" AND date_part('year'::text, dp.date) = cdd."year"
            LEFT JOIN exclusive_contract ec ON dp.id = ec.agent_id AND date_part('month'::text, dp.date) = ec."month" AND date_part('year'::text, dp.date) = ec."year"
            LEFT JOIN deal_count dc ON dp.id = dc.agents_id AND date_part('month'::text, dp.date) = dc."month" AND date_part('year'::text, dp.date) = dc."year"
            LEFT JOIN mortgage_count_leads mcl ON dp.id = mcl.id AND date_part('month'::text, dp.date) = mcl."month" AND date_part('year'::text, dp.date) = mcl."year"
            LEFT JOIN mortgage_consult_count mcc ON dp.id = mcc.id AND date_part('month'::text, dp.date) = mcc."month" AND date_part('year'::text, dp.date) = mcc."year"
            LEFT JOIN actual_objects ao ON dp.id = ao.responsible_id
            LEFT JOIN monthly_objects mo ON dp.id = mo.responsible_id AND date_part('month'::text, dp.date) = mo."month" AND date_part('year'::text, dp.date) = mo."year"
            LEFT JOIN cost_change_objects cco ON dp.id = cco.responsible_id AND date_part('month'::text, dp.date) = cco."month" AND date_part('year'::text, dp.date) = cco."year"
            LEFT JOIN bid_buy_count bbc ON dp.id = bbc.id AND date_part('month'::text, dp.date) = bbc."month" AND date_part('year'::text, dp.date) = bbc."year"
            LEFT JOIN ad_tables_count atc ON dp.id = atc.responsible_id
        WHERE 
            date_part('day', dp.date) = 5
            AND dp.experience > 0
            AND COALESCE(cdd.deals, 0) + COALESCE(ec.count, 0) + COALESCE(dc.deals, 0)
                + COALESCE(mcl.leads, 0) + COALESCE(mcc.leads, 0) + COALESCE(ao.objects_count, 0)
                + COALESCE(mo.objects_count, 0) + COALESCE(cco.cost_change_count, 0)
                + COALESCE(bbc.bids, 0) + COALESCE(atc.ads_count, 0) > 0
        GROUP BY 
            date_part('month'::text, dp.date),
            date_part('year'::text, dp.date),
            dp.id, 
            dp.full_name, 
            dp.department, 
            dp.position, 
            dp.career_start, 
            (date_part('year'::text, dp.date) - date_part('year'::text, career_start)) * 12::double precision + (date_part('month'::text, dp.date) - date_part('month'::text, career_start)),
            cdd.deals,
            ec.count,
            dc.deals,
            mcl.leads,
            mcc.leads,
            ao.objects_count,
            mo.objects_count,
            cco.cost_change_count,
            bbc.bids,
            atc.ads_count, 
            atc.avito_count, 
            atc.cian_count, 
            atc.yandex_count, 
            atc.domclick_count, 
            atc.avician_count
        UNION
        SELECT 
            date_part('month'::text, dp.date) AS month,
            date_part('year'::text, dp.date) AS year,
            dp.id, 
            dp.full_name, 
            dp.department, 
            dp.position, 
            (date_part('year'::text, dp.date) - date_part('year'::text, career_start)) * 12::double precision + (date_part('month'::text, dp.date) - date_part('month'::text, career_start)) AS experience,
            COALESCE(cdd.deals, 0) as nvstr,
            COALESCE(ec.count, 0) as exclusives,
            COALESCE(dc.deals, 0) as deals,
            COALESCE(mcl.leads, 0) as mortgage_leads,
            COALESCE(mcc.leads, 0) as mortgage_consult,
            COALESCE(ao.objects_count, 0) as all_objects,
            COALESCE(mo.objects_count, 0) as new_objects,
            COALESCE(cco.cost_change_count, 0) as cost_change_count,
            COALESCE(bbc.bids, 0) as bids,
            COALESCE(atc.ads_count, 0) as ads_count,
            COALESCE(atc.avito_count, 0) as avito_count,
            COALESCE(atc.cian_count, 0) as cian_count,
            COALESCE(atc.yandex_count, 0) as yandex_count,
            COALESCE(atc.domclick_count, 0) as domclick_count,
            COALESCE(cdd.deals, 0) + COALESCE(ec.count, 0) + COALESCE(dc.deals, 0)
                + COALESCE(mcl.leads, 0) + COALESCE(mcc.leads, 0) + COALESCE(ao.objects_count, 0)
                + COALESCE(mo.objects_count, 0) + COALESCE(cco.cost_change_count, 0)
                + COALESCE(bbc.bids, 0) + COALESCE(atc.ads_count, 0) as all_sum,
            COALESCE(atc.avician_count, 0) as avician
        FROM daily_personnel dp
            LEFT JOIN count_developer_deals cdd ON dp.id = cdd.responsible_id AND date_part('month'::text, dp.date) = cdd."month" AND date_part('year'::text, dp.date) = cdd."year"
            LEFT JOIN exclusive_contract ec ON dp.id = ec.agent_id AND date_part('month'::text, dp.date) = ec."month" AND date_part('year'::text, dp.date) = ec."year"
            LEFT JOIN deal_count dc ON dp.id = dc.agents_id AND date_part('month'::text, dp.date) = dc."month" AND date_part('year'::text, dp.date) = dc."year"
            LEFT JOIN mortgage_count_leads mcl ON dp.id = mcl.id AND date_part('month'::text, dp.date) = mcl."month" AND date_part('year'::text, dp.date) = mcl."year"
            LEFT JOIN mortgage_consult_count mcc ON dp.id = mcc.id AND date_part('month'::text, dp.date) = mcc."month" AND date_part('year'::text, dp.date) = mcc."year"
            LEFT JOIN actual_objects ao ON dp.id = ao.responsible_id
            LEFT JOIN monthly_objects mo ON dp.id = mo.responsible_id AND date_part('month'::text, dp.date) = mo."month" AND date_part('year'::text, dp.date) = mo."year"
            LEFT JOIN cost_change_objects cco ON dp.id = cco.responsible_id AND date_part('month'::text, dp.date) = cco."month" AND date_part('year'::text, dp.date) = cco."year"
            LEFT JOIN bid_buy_count bbc ON dp.id = bbc.id AND date_part('month'::text, dp.date) = bbc."month" AND date_part('year'::text, dp.date) = bbc."year"
            LEFT JOIN ad_tables_count atc ON dp.id = atc.responsible_id
        WHERE 
            (date_part('year'::text, now()) - date_part('year'::text, career_start)) * 12::double precision + (date_part('month'::text, now()) - date_part('month'::text, career_start)) = 0
            AND dp.date = now()::date - 2
            AND COALESCE(cdd.deals, 0) + COALESCE(ec.count, 0) + COALESCE(dc.deals, 0)
                + COALESCE(mcl.leads, 0) + COALESCE(mcc.leads, 0) + COALESCE(ao.objects_count, 0)
                + COALESCE(mo.objects_count, 0) + COALESCE(cco.cost_change_count, 0)
                + COALESCE(bbc.bids, 0) + COALESCE(atc.ads_count, 0) > 0
        GROUP BY 
            date_part('month'::text, dp.date),
            date_part('year'::text, dp.date),
            dp.id, 
            dp.full_name, 
            dp.department, 
            dp.position, 
            dp.career_start,
            cdd.deals,
            ec.count,
            dc.deals,
            (date_part('year'::text, now()) - date_part('year'::text, career_start)) * 12::double precision + (date_part('month'::text, now()) - date_part('month'::text, career_start)),
            mcl.leads,
            mcc.leads,
            ao.objects_count,
            mo.objects_count,
            cco.cost_change_count,
            bbc.bids,
            atc.ads_count, 
            atc.avito_count, 
            atc.cian_count, 
            atc.yandex_count, 
            atc.domclick_count, 
            atc.avician_count
    )
    INSERT INTO key_values (
        month, year, id, full_name, department, position, experience, nvstr, exclusives, deals, mortgage_leads, mortgage_consult, all_objects, new_objects, cost_change_count, bids, ads_count, avito_count, cian_count, yandex_count, domclick_count, all_sum, avician
    )
    SELECT 
        month, year, id, full_name, department, position, experience, nvstr, exclusives, deals, mortgage_leads, mortgage_consult, all_objects, new_objects, cost_change_count, bids, ads_count, avito_count, cian_count, yandex_count, domclick_count, all_sum, avician
    FROM source_data
    ON CONFLICT ("month", "year", id) DO UPDATE SET
        full_name = EXCLUDED.full_name, 
        department = EXCLUDED.department, 
        position = EXCLUDED.position,
        experience = EXCLUDED.experience,
        nvstr = EXCLUDED.nvstr,
        exclusives = EXCLUDED.exclusives,
        deals = EXCLUDED.deals,
        mortgage_leads = EXCLUDED.mortgage_leads,
        mortgage_consult = EXCLUDED.mortgage_consult,
        all_objects = EXCLUDED.all_objects,
        new_objects = EXCLUDED.new_objects,
        cost_change_count = EXCLUDED.cost_change_count,
        bids = EXCLUDED.bids,
        ads_count = EXCLUDED.ads_count, 
        avito_count = EXCLUDED.avito_count, 
        cian_count = EXCLUDED.cian_count, 
        yandex_count = EXCLUDED.yandex_count, 
        domclick_count = EXCLUDED.domclick_count,
        all_sum = EXCLUDED.all_sum,
        avician = EXCLUDED.avician;

    GET DIAGNOSTICS rows_processed = ROW_COUNT;
    RETURN rows_processed;
END;
$function$
;
