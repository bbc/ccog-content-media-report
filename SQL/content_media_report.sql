------------------------------------------------------------------------------------------------------------------------
--- Digital spend  -----------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------

--- Get current week digital spend  ------------------------------------------------------------------------------------
drop table if exists digital_spend;
create temp table digital_spend as
select date_trunc('week', date)::DATE as wc_monday,
       at_brand,
       at_product,
       sum(ctc) as spend
from marketing_insights.bbc_mna_oc_client_database
where wc_monday = /*'2025-07-21'*/ '<params.run_date>'
  and at_product = 'iplayer'
group by 1, 2, 3
having sum(ctc) > 100; --change to the week macro in MAP

--- Upsert current week into historical digital spend  -----------------------------------------------------------------
delete from marketing_insights.in_content_digital_spend
where wc_date in (select distinct wc_date from digital_spend);

insert into marketing_insights.in_content_digital_spend
select *
from digital_spend;

------------------------------------------------------------------------------------------------------------------------
--- Owned impressions  -------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------

--- Get current week owned impressions  --------------------------------------------------------------------------------
drop table if exists owned_impressions;
create temp table owned_impressions as
with step_1 as (SELECT date_trunc('week', dt::date) as wc_monday,
                       item_link,
                       count(audience_id) as impressions
                FROM s3_audience.audience_activity
                WHERE source = 'Custom'
                  AND wc_monday = /*'20250721'*/
                    to_char(('<params.run_date>')::DATE, 'YYYYMMDD')
                  AND app_type = 'responsive'
                  AND event_action = 'view'
                  AND (item_link LIKE '%%xtor=CS8-1000%%'
                    OR (item_link LIKE '%%at_medium=owned_display%%' AND item_link LIKE '%%at_campaign_type=owned%%')
                    OR (item_link LIKE '%%at_medium=display_ad%%' AND item_link LIKE '%%at_campaign_type=owned%%')
                    OR (item_link LIKE '%%at_medium=display%%' AND item_link LIKE '%%at_campaign_type=owned%%')
                    )
                  AND ((item_link LIKE '%%at_product=iplayer%%') OR (item_link LIKE '%%at_product=sounds%%'))
                GROUP BY 1, 2)

SELECT wc_monday,
       REGEXP_SUBSTR(item_link, '^.*[?&]at_brand=([a-zA-Z0-9]*)[^&at_]?', 1, 1,
                     'e')                                                               AS at_brand,
       REGEXP_SUBSTR(item_link, '^.*[?&]at_product=([a-zA-Z0-9_]*)[^&at_]?', 1, 1, 'e') AS product_promoted,
       sum(impressions)                                                                 as impressions
FROM step_1
where product_promoted ilike 'iplayer'
and impressions > 10000
GROUP BY 1, 2, 3;

--- Upsert current week into historical owned impressions  -------------------------------------------------------------
delete from marketing_insights.in_content_owned_impressions
where wc_date in (select distinct wc_date from owned_impressions);

insert into marketing_insights.in_content_owned_impressions
select * from owned_impressions;

------------------------------------------------------------------------------------------------------------------------
--- TV -----------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------

--- Get current week owned tvrs  ---------------------------------------------------------------------------------------
drop table if exists owned_tvrs;
create temp table owned_tvrs as
select date_trunc('week', date) as wc_monday,
       case
           when (accutics_product_promoted ilike '%iplayer%' or
                 accutics_product_promoted in
                 ('news', 'sport', 'cbbc', 'cbeebies', 'pan_bbc', 'programmes'))
               then 'iplayer'
           when accutics_product_promoted ilike '%sounds%' then 'sounds'
           else 'other' end     as product_promoted,
       accutics_brand_id        as at_brand,
       sum(tvr_adults_16_plus)  as tvrs
from marketing_insights.in_tv_enriched
where wc_monday = '<params.run_date>'
  and accutics_product_promoted = 'iplayer'
group by 1, 2, 3;

--- Upsert current week into historical owned tvrs  --------------------------------------------------------------------
delete from marketing_insights.in_content_owned_tvrs
where wc_date in (select distinct wc_date from owned_tvrs);

insert into marketing_insights.in_content_owned_tvrs
select *
from owned_tvrs;

------------------------------------------------------------------------------------------------------------------------
--- Join all tables  ---------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------
drop table if exists media_joined;
create temp table media_joined as
SELECT COALESCE(a.wc_monday, b.wc_monday, c.wc_monday) AS wc_date,
       COALESCE(a.at_brand, b.at_brand, c.at_brand)    AS at_brand,
       SUM(COALESCE(a.spend, 0))                       AS digital_spend,
       SUM(COALESCE(b.impressions, 0))                 AS impressions,
       SUM(COALESCE(c.tvrs, 0))                        AS tvrs
FROM marketing_insights.in_content_digital_spend a
         FULL OUTER JOIN marketing_insights.in_content_owned_impressions b
                         ON a.at_brand = b.at_brand
                             AND a.wc_monday = b.wc_monday

         FULL OUTER JOIN marketing_insights.in_content_owned_tvrs c
                         ON a.at_brand = c.at_brand
                             AND a.wc_monday = c.wc_monday
group by 1, 2;


------------------------------------------------------------------------------------------------------------------------
--- Normalise Media vars all tables  -----------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------
drop table if exists norm_media;
create temp table norm_media as
with get_media as (select wc_date,
                          at_brand,
                          sum(digital_spend) as digital_spend,
                          sum(impressions)   as impressions,
                          sum(tvrs)          as tvrs
                   from media_joined
                   group by 1, 2),
     stats_1 as (select avg(digital_spend)    as avg_spend,
                        stddev(digital_spend) as std_spend,
                        avg(impressions)      as avg_imps,
                        stddev(impressions)   as std_imps,
                        avg(tvrs)             as avg_tvrs,
                        stddev(tvrs)          as std_tvrs
                 from get_media),
     norm as (select a.*,
                     (a.digital_spend - b.avg_spend) / b.std_spend as norm_spend,
                     (a.impressions - b.avg_imps) / b.std_imps     as norm_imps,
                     (a.tvrs - b.avg_tvrs) / b.std_tvrs            as norm_tvrs
              from get_media a
                       cross join stats_1 b)
select *,
       (norm_spend + norm_imps + norm_tvrs) / 3 as average_total_norm
from norm;

select *
from norm_media;

------------------------------------------------------------------------------------------------------------------------
--- Enrich with brand_titles -------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------
drop table if exists media_enriched;
create temp table media_enriched as
with get_titles as (select distinct case
                                        when (brand_id is not null and brand_id != 'null') then brand_id
                                        when (series_id is not null and series_id != 'null') then series_id
                                        else episode_id
                                        end as pid,
                                    case
                                        when (brand_title is not null and brand_title != 'null') then brand_title
                                        when (series_title is not null and series_title != 'null') then series_title
                                        else episode_title
                                        end as title
                    from prez.scv_vmb
                    where partner_name = 'British Broadcasting Corporation'),
     get_norm_media as (select wc_date,
                               a.at_brand,
                               b.title,
                               sum(digital_spend)    as digital_spend,
                               sum(impressions)      as impressions,
                               sum(tvrs)             as tvrs,
                               sum(average_total_norm) as average_norm_media
 from norm_media a
                                 left join get_titles b
                                           on a.at_brand = b.pid
                        where (title != '' or title is not null)
                        group by 1, 2, 3),
     get_percentile as (select *,
                               ntile(10) over (order by average_norm_media) as percentile_average
                        from get_norm_media)
select *,
       case
           when percentile_average >= 1 and percentile_average <= 2 then 'low'
           when percentile_average >= 3 and percentile_average <= 8 then 'medium'
           when percentile_average >= 9 then 'high'
           end as media_average_label
from get_percentile
where wc_date BETWEEN
    date_trunc('week', ) - interval '15 weeks'
    AND date_trunc('week', )
order by wc_date, average_norm_media;


------------------------------------------------------------------------------------------------------------------------
--- Unload Report to S3 ------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------

UNLOAD ('SELECT * FROM media_enriched')
TO 's3://map-input-output/nj-ccog-content-media-report'
CREDENTIALS 'aws_access_key_id=<params.AWS_ACCESS_KEY_ID>;aws_secret_access_key=<params.AWS_SECRET_ACCESS_KEY>;token=<params.TOKEN>'
CSV
GZIP
ALLOWOVERWRITE
HEADER
PARALLEL OFF;









