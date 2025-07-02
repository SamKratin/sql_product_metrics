--to be able to implement the decoding function right away in the main task, we'll start with writing it
create or replace function url_decode(text) returns text as $$
declare
    input alias for $1;
    result text := '';
    i int := 1;
    hex text;
begin
    while i <= length(input) loop
        if substring(input, i, 1) = '%' then
            hex := substring(input, i + 1, 2);
            result := result || convert_from(decode(hex, 'hex'), 'UTF8');
            i := i + 3;
        elsif substring(input, i, 1) = '+' then
            result := result || ' ';
            i := i + 1;
        else
            result := result || substring(input, i, 1);
            i := i + 1;
        end if;
    end loop;
    return result;
end;
$$ language plpgsql;

--max length of umt_campaign to see the amouont of characters 
with check_the_len as (
select fabd.url_parameters 
from facebook_ads_basic_daily as fabd
union all
select gabd.url_parameters 
from google_ads_basic_daily gabd
)
select substring(url_parameters, 'utm_campaign=([^&#$]+)') AS raw_campaign
	  ,length(substring(url_parameters, 'utm_campaign=([^&#$]+)')) as lengthhh
from check_the_len
group by raw_campaign
order by lengthhh desc;

/*
 The max lenght of the regular (non crypted) campaign is 14 chars
 */


--let's implement the kpi's calculation with utm decoding 
with main_table as (
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
)
select ad_date
	  ,lower(
	  case 
	  	   when substring(url_parameters, 'utm_campaign=([^&#$]+)') = 'nan'
	  	   then null
	  	   when length(substring(url_parameters, 'utm_campaign=([^&#$]+)')) > 14
	  	   then url_decode(substring(url_parameters, 'utm_campaign=([^&#$]+)'))
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
	  end || '%' as CTR
	  ,case 
	  	when sum(spend) <> 0
	  	then round((sum(value::numeric) - sum(spend::numeric)) / sum (spend::numeric) * 100, 2)
	  end || '%' as ROMI	  
from main_table
where ad_date is not null
group by 1, 2;



	   	
	   