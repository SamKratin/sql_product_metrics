--product metrics calculation
select ad_date
	   ,campaign_id 
	   ,sum(spend) as ttl_spend
	   ,sum(impressions) as ttl_impressions
	   ,sum(clicks) as ttl_clicks
	   ,sum(value) as ttl_value
	   ,round(sum(spend::numeric) / sum(clicks::numeric), 2) as CPC
	   ,round((sum(spend::numeric) / sum(impressions::numeric) * 1000), 2) as CPM
	   ,round((sum(clicks::numeric) / sum(impressions::numeric) * 100),2) as CTR
	   ,round((sum(value::numeric) - sum(spend::numeric)) / sum (spend::numeric), 2) as ROMI
from facebook_ads_basic_daily
where clicks != 0 and impressions != 0 and spend != 0
group by ad_date, campaign_id 
order by ad_date;

--campaign with ttl spend over 500k and the highest ROMI
select campaign_id
	   ,ROMI
	from (select campaign_id 
		   ,sum(spend) as ttl_spend
		   ,sum(impressions) as ttl_impressions
		   ,sum(clicks) as ttl_clicks
		   ,sum(value) as ttl_value
		   ,round(sum(spend::numeric) / sum(clicks::numeric), 2) as CPC
		   ,round((sum(spend::numeric) / sum(impressions::numeric) * 1000), 2) as CPM
		   ,round((sum(clicks::numeric) / sum(impressions::numeric) * 100),2) as CTR
		   ,round((sum(value::numeric) - sum(spend::numeric)) / sum (spend::numeric),2) as ROMI
	from facebook_ads_basic_daily
	group by campaign_id 
	having sum(clicks) != 0 and sum(impressions) != 0 and sum(spend) != 0) as t1
where ttl_spend > 500000
order by ROMI desc
limit 1;
