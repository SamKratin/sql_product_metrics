-- This section calculates the average, maximum, and minimum daily spend
-- separately for Google and Facebook campaigns to assess spending behavior by platform.
-- Grouping is done by ad_date and platform.
with cte as (
select ad_date
      ,spend 
      ,'Google' as platform
from google_ads_basic_daily as gabd
union all
select ad_date
       ,spend
       ,'Facebook' as platform
from facebook_ads_basic_daily as fabd 
)
select ad_date
	   ,platform
       ,round(avg(spend), 2) as average_daily_spend
       ,max(spend) as max_daily_spend
       ,min(spend) as min_daily_spend
from cte 
group by 1, 2
order by ad_date;

-- This section identifies the five most profitable days in terms of ROMI
-- across both Google and Facebook platforms.
-- Results are ordered in descending ROMI value.
with cte_1 as (
select ad_date
	   ,coalesce(value, 0) as value
       ,coalesce(spend, 0) as spend
       ,'Google' platform
from google_ads_basic_daily 
union all
select ad_date
 	   ,coalesce(value, 0) as value
 	   ,coalesce(spend, 0) as spend
 	   ,'Facebook' platform
from facebook_ads_basic_daily
),
cte_2 as (
select  ad_date
       ,platform
       ,case 
       	when sum(spend) <> 0
       	then round((sum(value::numeric) - sum(spend::numeric)) / sum(spend::numeric) * 100, 2)
        end as ROMI
from cte_1
group by 1, 2 
)
select ad_date
	   ,platform
	   ,ROMI  || '%' as romi_percentage
from cte_2
where ROMI is not null
order by romi desc
limit 5;

-- This section calculates total value per campaign per week
-- and returns the campaign with the highest weekly revenue.
-- Useful for identifying top-performing campaigns.
with cte as (
select ad_date
	   ,value
	   ,campaign_name
from facebook_ads_basic_daily as fabd
left join facebook_campaign f
on fabd.campaign_id = f.campaign_id
union all
select ad_date
       ,value
       ,campaign_name
from google_ads_basic_daily
),
cte2 as (
select campaign_name
	   ,date_trunc('week', ad_date) as week
	   ,sum(value) as sum_value
from cte 
group by campaign_name, date_trunc('week', ad_date))
select campaign_name,
	   week,
	   sum_value
from cte2
where campaign_name is not null
order by sum_value desc
limit 1;

-- Using a LAG function, this section compares monthly reach values
-- and determines which campaign had the greatest increase in reach month-over-month.

with cte as (
select ad_date
       ,coalesce(reach, 0) as reach
       ,campaign_name
from facebook_ads_basic_daily as fabd
left join facebook_campaign f
on fabd.campaign_id = f.campaign_id

union all

select ad_date
       ,coalesce(reach, 0) as reach
       ,campaign_name
from google_ads_basic_daily
),
cte2 as (
select date_trunc('month', ad_date) as month 
       ,campaign_name
       ,sum(reach) as reach
from cte
group by 1, 2
),
cte3 as (
select month 
      ,campaign_name
      ,reach - lag(reach) over (partition by campaign_name order by month) as reach_change
from cte2
where campaign_name is not null 
)
select campaign_name
       ,sum(reach_change) as ttl_growth_of_campaign
from cte3
group by campaign_name
order by ttl_growth_of_campaign desc
limit 1;

-- This section returns the adset_name with the longest streak
-- of consecutive daily appearances, across both Google and Facebook platforms.
-- Helps identify the most consistently active ad set.
with cte as ( 
select ad_date
      ,adset_name
from facebook_ads_basic_daily fabd
left join facebook_adset f 
on fabd.adset_id = f.adset_id
union -- to avoid duplicates for the row_number functon
select ad_date
	  ,adset_name
from google_ads_basic_daily
),
cte2 as (
select date_trunc('day', ad_date) date
       ,adset_name
from cte
where ad_date is not null
), 
cte3 as (
select date 
      ,adset_name 
	  ,row_number() over (partition by adset_name order by date) as rank
	  ,date - (row_number() over (partition by adset_name order by date) || ' day')::interval as new_key
from cte2
),
cte4 as(
select adset_name
      ,new_key
      ,min(date) as start_date
      ,max(date) as end_date
      ,count(*) as duration_day
from cte3
group by adset_name, new_key 
)
select adset_name
	  ,max(duration_day) as max_duration_day
from cte4
group by adset_name
order by 2 desc
limit 5; --'narrow' adset has the longest duration - 108 days