-- monthly campaign performance tracking. Tracking cpm, ctr, romi metrics.
with first_ste as (
select fabd.ad_date
	  ,fabd.url_parameters 
	  ,coalesce(fabd.spend, 0) as spend
	  ,coalesce(fabd.impressions, 0) as impressions
	  ,coalesce(fabd.reach, 0) as reach
	  ,coalesce(fabd.clicks, 0) as clicks
	  ,coalesce(fabd.leads, 0) as leads
	  ,coalesce(fabd.value, 0) as value
from facebook_ads_basic_daily as fabd
union all
select gabd.ad_date
	  ,gabd.url_parameters 
	  ,coalesce(gabd.spend, 0) as spend
	  ,coalesce(gabd.impressions, 0) as impressions
	  ,coalesce(gabd.reach, 0) as reach
	  ,coalesce(gabd.clicks, 0) as clicks
	  ,coalesce(gabd.leads, 0) as leads
	  ,coalesce(gabd.value, 0) as value
from google_ads_basic_daily gabd
),
second_ste as (
select date_trunc('month', ad_date) as ad_month
	  ,lower(
	  case 
	  	   when substring(url_parameters, 'utm_campaign=([^&#$]+)') = 'nan'
	  	   then null
	  	   else substring(url_parameters, 'utm_campaign=([^&#$]+)')
	   end) as utm_campaign
	  ,sum(spend) as ttl_spend
	  ,sum(impressions) as ttl_impressions
	  ,sum(clicks) as ttl_clicks
	  ,sum(leads) as ttl_leads
	  ,sum(value) as ttl_value
	  ,case 
	  	when sum(clicks) <> 0 
	  	then round(sum(spend::numeric) / sum(clicks::numeric), 2)
	  end as CPC
	  ,case 
	  	when sum(impressions) <> 0 
	  	then round((sum(spend::numeric) / sum(impressions::numeric) * 1000), 2)
	  end as CPM
	  ,case 
	  	when sum(impressions) <> 0 
	  	then round((sum(clicks::numeric) / sum(impressions::numeric) * 100), 2)
	  end as CTR
	  ,case 
	  	when sum(spend) <> 0
	  	then round((sum(value::numeric) - sum(spend::numeric)) / sum (spend::numeric) * 100, 2)
	  end as ROMI	  
from first_ste
where 2 is not null
group by 1, 2
)
select ad_month
	  ,utm_campaign
	  ,ttl_spend
	  ,ttl_impressions 
	  ,ttl_clicks 
	  ,ttl_value 
	  ,cpc
	  ,cpm
	  ,ctr
	  ,romi 
	  ,case 
	  	when lag(cpm, 1) over (partition by utm_campaign) <> 0
	  	then round(((cpm / lag(cpm, 1) over (partition by utm_campaign order by utm_campaign, ad_month)) - 1) * 100)
	  end || '%' as  cpm_diff_lst_month
	  ,case
	  	when lag(ctr, 1) over (partition by utm_campaign) <> 0
	  	then round(((ctr / lag(ctr, 1) over (partition by utm_campaign order by utm_campaign, ad_month)) - 1) * 100)
	  end || '%' as ctr_diff_lst_month
	  ,case
	  	when lag(romi, 1) over (partition by utm_campaign) <> 0
	  	then round(((romi / lag(romi, 1) over (partition by utm_campaign order by utm_campaign, ad_month)) - 1) * 100)
	  end || '%' romi_diff_lst_month
from second_ste
where ad_month is not null

