-- ##############################################################
/* Google Merchandise Store Funnel Analysis for 2021

This query analyzes user behavior across the entire purchase funnel 
on the Google Merchandise Store website for the year 2021.

It includes:
1. Extraction of session-level and event-level details 
2. Funnel conversion metrics by campaign/source/medium
3. Conversion rate per landing page 

The analysis helps identify:
How many sessions progress through add-to-cart, checkout, and purchase
Which campaigns bring the most valuable users
Which landing pages lead to purchases

Source: BigQuery Public Dataset (ga4_obfuscated_sample_ecommerce)
Period analyzed: 2021-01-01 to 2021-12-31 */
-- ##############################################################


-- This query retrieves timestamp, user ID, session ID, event name, 
-- country, device type, source, medium, and campaign name for a limited set of user interactions in 2021.
select timestamp_micros(event_timestamp) as date
      ,user_pseudo_id
      ,(select value.int_value 
       from unnest(event_params)
       where key = 'ga_session_id') as session_id
      ,event_name
      ,geo.country as country
      ,device.category as device_category
      ,traffic_source.source as source
      ,traffic_source.medium as medium
      ,(select value.string_value
        from unnest(event_params)
        where key = 'campaign') as campaign
from `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
where _table_suffix >= '20210101' and _table_suffix <= '20211231'
and event_name in ('session_start', 'view_item', 'add_to_cart', 'begin_checkout', 'add_shipping_info', 'add_payment_info', 'purchase')
limit 5;


-- The query tracks funnel stages (session → cart → checkout → purchase) 
-- using distinct session counts and computes corresponding conversion rates per day.
with cte as (
select date(timestamp_micros(event_timestamp)) as event_date
      ,traffic_source.source as source
      ,traffic_source.medium as medium
      ,traffic_source.name as campaign
      ,event_name
      ,concat(user_pseudo_id, (select value.int_value 
       from unnest(event_params)
       where key = 'ga_session_id')) as user_sessions
from `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
where _table_suffix between '20210101' and '20211231'
),
cte_2 as (
select  event_date 
       ,campaign
       ,source
       ,medium
       ,count(distinct 
               case
               when event_name = 'session_start'
               then user_sessions
               end) as user_sessions_count 
       ,count(distinct 
               case
               when event_name = 'add_to_cart'
               then user_sessions
               end) as add_to_cart_sessions_count 
       ,count(distinct 
               case
               when event_name = 'begin_checkout'
               then user_sessions
               end) as checkout_sessions_count 
       ,count(distinct 
               case
               when event_name = 'purchase'
               then user_sessions
               end) as purchase_sessions_count 
from cte
group by 1, 2, 3, 4
)
select  event_date
       ,campaign
       ,source
       ,medium
       ,user_sessions_count 
       ,round((add_to_cart_sessions_count / user_sessions_count) * 100, 2) || '%' as visit_to_cart
       ,round((checkout_sessions_count / user_sessions_count ) * 100, 2) || '%' as visit_to_checkout
       ,round((purchase_sessions_count  / user_sessions_count ) * 100, 2) || '%' as visit_to_purchase 
from cte_2;


-- This query extracts the page path for each session start event, 
-- joins it with purchase events based on user-session ID, and computes conversion rate per page.
with table_1 as (
select  user_pseudo_id
        ,concat(user_pseudo_id, (select value.int_value 
        from unnest(event_params)
        where key = 'ga_session_id')) as user_sessions
        ,replace((select value.string_value
        from unnest(event_params)
        where key = 'page_location'), 'https://shop.googlemerchandisestore.com/', '') as page_path
from `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*` as s
where _table_suffix between '20210101' and '20211231'
and event_name = 'session_start'
),
table_2 as (
select  user_pseudo_id
        ,concat(user_pseudo_id, (select value.int_value 
        from unnest(event_params)
        where key = 'ga_session_id')) as user_sessions
from`bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*` as p
where _table_suffix between '20210101' and '20211231'
and event_name = 'purchase'
),
final as (
select  page_path
        ,count(distinct s.user_sessions) AS session_count
        ,count(distinct s.user_pseudo_id) AS unique_users
        ,count(distinct p.user_sessions) AS purchase_count
from table_1 s
left join table_2 p 
on s.user_sessions = p.user_sessions
group by s.page_path
)
select  page_path 
       ,session_count
       ,unique_users 
       ,purchase_count
       ,round (case 
                when session_count = 0 
                then null 
                else (purchase_count / session_count) *100 end, 2) as conversion_rate
from final;