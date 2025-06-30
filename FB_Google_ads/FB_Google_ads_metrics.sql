-- Combining FB and Google Ads data with daily metrics (spend, impressions, clicks, reach etc)
-- Including campaign and ad set names, and labeling each row with its media source
with fb_set as (
select fabd.ad_date
	  ,'Facebook ads' as media_source
	  ,fc.campaign_name 
	  ,fa.adset_name 
	  ,fabd.spend 
	  ,fabd.impressions
	  ,fabd.reach
	  ,fabd.clicks 
	  ,fabd.leads 
	  ,fabd.value
from facebook_ads_basic_daily as fabd
left join facebook_adset as fa
	on fabd.adset_id = fa.adset_id
left join facebook_campaign as fc
	on fabd.campaign_id = fc.campaign_id 
union all
select gabd.ad_date
	  ,'Google ads' as media_source
	  ,fc.campaign_name 
	  ,fa.adset_name 
	  ,gabd.spend 
	  ,gabd.impressions
	  ,gabd.reach
	  ,gabd.clicks 
	  ,gabd.leads 
	  ,gabd.value
from google_ads_basic_daily gabd
left join facebook_adset as fa
	on gabd.adset_name  = fa.adset_name 
left join facebook_campaign as fc
	on gabd.campaign_name = fc.campaign_name 
)
select ad_date
	  ,media_source 
	  ,campaign_name 
	  ,adset_name 
	  ,sum(spend) as ttl_spend
	  ,sum(impressions) as ttl_impressions
	  ,sum(clicks) as ttl_clicks
	  ,sum(value) as ttl_conversions
	  ,round(sum(spend::numeric) / sum(clicks::numeric), 2) as CPC
	  ,round((sum(spend::numeric) / sum(impressions::numeric) * 1000), 2) as CPM
	  ,round((sum(clicks::numeric) / sum(impressions::numeric) * 100),2) as CTR
	  ,round((sum(value::numeric) - sum(spend::numeric)) / sum (spend::numeric), 2) as ROMI
from fb_set
where spend <> 0 and impressions <> 0 and clicks <> 0
group by ad_date
	  ,media_source 
	  ,campaign_name 
	  ,adset_name;

-- Finding out the highest ROMI
with highest_romi as (
select fabd.ad_date
	  ,'Facebook ads' as media_source
	  ,fc.campaign_name 
	  ,fa.adset_name 
	  ,fabd.spend 
	  ,fabd.impressions
	  ,fabd.reach
	  ,fabd.clicks 
	  ,fabd.leads 
	  ,fabd.value
from facebook_ads_basic_daily as fabd
left join facebook_adset as fa
	on fabd.adset_id = fa.adset_id
left join facebook_campaign as fc
	on fabd.campaign_id = fc.campaign_id 
union all
select gabd.ad_date
	  ,'Google ads' as media_source
	  ,fc.campaign_name 
	  ,fa.adset_name 
	  ,gabd.spend 
	  ,gabd.impressions
	  ,gabd.reach
	  ,gabd.clicks 
	  ,gabd.leads 
	  ,gabd.value
from google_ads_basic_daily gabd
left join facebook_adset as fa
	on gabd.adset_name  = fa.adset_name 
left join facebook_campaign as fc
	on gabd.campaign_name = fc.campaign_name 
)
select adset_name
	  ,round((sum(value::numeric) - sum(spend::numeric)) / sum (spend::numeric) * 100, 2) || '%' as romi
from highest_romi
group by campaign_name, adset_name
having sum(spend) > 500000
order by romi desc
limit 1;





