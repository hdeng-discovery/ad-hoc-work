with a as (	
select distinct 	
	CAST(DATEADD ('day',-1,DATEADD ('month',1,DATE_TRUNC('month',report_date))) AS DATE) as month_ending,	
	user_id	
from thunder_load.dwh_sonic_subscriptions_mv	
	where 	
	new_acquisition_ind=1  AND 	
	(subs_product LIKE 'artemis-dtc-monthly-ads' or subs_product LIKE '%limited-ads%') and 	
	month_ending in ('2021-01-31','2021-04-30')	
),	
b as (	
select distinct month_ending,a.user_id, ecid	
from a 	
join thunder_load.dwh_sonic_subscriptions_mv_current b on a.user_id=b.user_id and closing_active_ind=1 and 	
(subs_product LIKE 'artemis-dtc-monthly-ads' or subs_product LIKE '%limited-ads%')	
join thunder_load.dwh_adobe_daily_on_demand_streaming_dtc c on a.user_id=c.user_id	
where date>='2021-04-01' and login_status='authenticated' 	
order by random()	
)	
, c as (	
select distinct 	
ecid,
case 
	when month_ending='2021-01-31' and row_number() over (partition by month_ending) <= 89205 then 'd_sid=22212453' 
	when month_ending='2021-01-31' and row_number() over (partition by month_ending) <= 178410 then 'd_sid=22234017'
	when month_ending='2021-04-30' and row_number() over (partition by month_ending) <= 33122 then 'd_sid=22212457' 
	when month_ending='2021-04-30' and row_number() over (partition by month_ending) <= 66244 then 'd_sid=22234024'
	end as g	
from b 	
)	
select * 	
from c 	
where g is not null;

-- JUNE COHORT
drop table digital_analytics_dev.ad_load_test_jul18;
create table digital_analytics_dev.ad_load_test_jul18 as
with a as (	
select distinct 	
	CAST(DATEADD ('day',-1,DATEADD ('month',1,DATE_TRUNC('month',report_date))) AS DATE) as month_ending,	
	CAST(DATEADD ('day',-1,DATEADD ('week',1,DATE_TRUNC('week',report_date))) AS DATE) as week_ending,	
	user_id	
from thunder_load.dwh_sonic_subscriptions_mv	
	where 	
	new_acquisition_ind=1  AND 	
	(subs_product LIKE 'artemis-dtc-monthly-ads' or subs_product LIKE '%limited-ads%') and 	
	month_ending in ('2021-06-30')	
), 
b as (	
select  month_ending,week_ending,a.user_id,sum(total_minutes) total_minutes
from a 	
join thunder_load.dwh_sonic_subscriptions_mv_current b on a.user_id=b.user_id and closing_active_ind=1 and 	
(subs_product LIKE 'artemis-dtc-monthly-ads' or subs_product LIKE '%limited-ads%')	
join thunder_load.dwh_sonic_subscriptions_mv d on a.user_id=d.user_id and d.closing_active_ind=1 and 	
(d.subs_product LIKE 'artemis-dtc-monthly-ads' or d.subs_product LIKE '%limited-ads%')	and d.report_date='2021-07-01'
join thunder_load.dwh_adobe_daily_on_demand_streaming_dtc c on a.user_id=c.user_id and date>='2021-07-01' and nvl(ad_minutes,0)>0
group by 1,2,3
)	,
c as (
	select
	month_ending,
	week_ending,
	user_id,
	total_minutes,
	ntile(10) over (partition by week_ending order by total_minutes desc) decile
	from b
), 
d as (
	select distinct 
	month_ending,
	week_ending,
	user_id,
	decile,
	total_minutes,
	row_number() over (partition by decile,week_ending order by random()) as num
	from c ),
e as (
	select distinct 
	month_ending,
	week_ending,
	user_id,
	decile,
	total_minutes,
	case 
		when num<=500 then '4m' 
		when num<=1000 then '5m'
		end as group_name	
	from d
)
select distinct e.*,ecid 
from e
join thunder_load.dwh_adobe_daily_on_demand_streaming_dtc t 
on e.user_id=t.user_id and date>='2021-07-01' and nvl(ad_minutes,0)>0 and group_name is not null;

create table #t as
with a as 
(
select distinct ecid
from digital_analytics_dev.ad_load_test_jul18
group by 1
having count(distinct user_id)>1
)
select * from digital_analytics_dev.ad_load_test_jul18
where ecid in (select distinct ecid from a)
;

select decile,group_name,count(distinct user_id) 
from #t
group by 1,2;

delete from digital_analytics_dev.ad_load_test_jul18 where ecid in (select distinct ecid from #t);

select distinct ecid,case when group_name='5m' then 22813889 else 22813877 end as trait_id
from digital_analytics_dev.ad_load_test_jul18;


insert into digital_analytics_dev.ad_load_test (ecid,test_group)
select distinct 
ecid,case when group_name = '5m' then 'jun_5m_ads'
when group_name = '4m' then 'jun_4m_ads' end as test_group
from digital_analytics_dev.ad_load_test_jul18

create table digital_analytics_dev.ad_load_test (ecid varchar(60),icc varchar(10));

copy digital_analytics_dev.ad_load_test from 's3://dci-prod-dataanalytics-teams-datastrategy-us-east-1/hdeng/abtest/ftp_dpm_1136184_1620056700.csv' iam_role 'arn:aws:iam::246607762912:role/prod-us-analytics-redshift-teams'  csv
EMPTYASNULL IGNOREHEADER 1 delimiter '\t';

alter table digital_analytics_dev.ad_load_test
add column test_group varchar(20);
update digital_analytics_dev.ad_load_test
set test_group = case 
when icc='22234024' then 'apr_5m_ads'
when icc='22212457' then 'apr_4m_ads'
when icc='22212453' then 'jan_4m_ads'
when icc='22234017' then 'jan_5m_ads' end;

-- potential churn
select 
	test_group,
	count(distinct c.user_id)
from digital_analytics_dev.ad_load_test a
join thunder_load.dwh_adobe_daily_on_demand_streaming_dtc c 
on a.ecid=c.ecid and a.ecid is not null and nvl(ad_minutes,0)>0 and 
login_status='authenticated' and date>='2021-05-06' 
join thunder_load.dwh_sonic_subscriptions_mv b 
on c.user_id=b.user_id  and 	closing_active_ind=1 and report_date='2021-05-05' and 
	(b.subs_product LIKE 'artemis-dtc-monthly-ads' or b.subs_product LIKE '%limited-ads%')	
join thunder_load.dwh_sonic_subscriptions_mv_current d 
on c.user_id=d.user_id  and 	d.closing_active_ind=1 and 
	(d.subs_product LIKE 'artemis-dtc-monthly-ads' or d.subs_product LIKE '%limited-ads%')	
group by 1
order by 1;

select count(distinct c.user_id)
from thunder_load.dwh_sonic_subscriptions_mv b 
join thunder_load.dwh_adobe_daily_on_demand_streaming_dtc c 
on nvl(ad_minutes,0)>0   and c.user_id=b.user_id  and 	closing_active_ind=1 and report_date='2021-05-05' and 
	(b.subs_product LIKE 'artemis-dtc-monthly-ads' or b.subs_product LIKE '%limited-ads%')	AND 
	login_status='authenticated' and date>='2021-05-06'
join thunder_load.dwh_sonic_subscriptions_mv_current d 
on c.user_id=d.user_id  and 	d.closing_active_ind=1 and 
	(d.subs_product LIKE 'artemis-dtc-monthly-ads' or d.subs_product LIKE '%limited-ads%');

-- churn by quartile/minutes_bucket
with users as (
select distinct user_id
from thunder_load.dwh_sonic_subscriptions_mv 
where report_date between '2021-04-10' and '2021-05-05' and 
(subs_product LIKE 'artemis-dtc-monthly-ads' or subs_product LIKE '%limited-ads%')  
group by 1
having sum(closing_active_ind)=26
),
a as (
select 
  test_group,a.user_id,
  sum(total_minutes)/26.0 total_mins_prior
from thunder_load.dwh_adobe_daily_on_demand_streaming_dtc a
join users b on a.user_id=b.user_id and nvl(ad_minutes,0)>0
join digital_analytics_dev.ad_load_test c on a.ecid=c.ecid
where 
  login_status='authenticated' and 
  date between '2021-04-10' and '2021-05-05'
group by 1,2
),
b as (
select 
  test_group,user_id,total_mins_prior,
  case when total_mins_prior<10 then '<10'
  when total_mins_prior between 10 and 30 then '10-30'
  when total_mins_prior between 30 and 60 then '30-60'
  when total_mins_prior between 60 and 100 then '60-100'
  when total_mins_prior >100 then '>100' end as minutes_bucket
  -- ntile(4) over (partition by a.test_group order by total_mins_prior desc) quartile
from a)
select
  b.test_group,
  b.minutes_bucket,
  -- median_minutes,
  count(distinct c.user_id) subs_active_0505,
  count(distinct d.user_id) subs_active_0627
from b 
join thunder_load.dwh_sonic_subscriptions_mv c 
on c.user_id=b.user_id  and closing_active_ind=1 and report_date='2021-05-05' and 
  (c.subs_product LIKE 'artemis-dtc-monthly-ads' or c.subs_product LIKE '%limited-ads%')  
left join thunder_load.dwh_sonic_subscriptions_mv_current d 
on c.user_id=d.user_id  and d.closing_active_ind=1 and 
  (d.subs_product LIKE 'artemis-dtc-monthly-ads' or d.subs_product LIKE '%limited-ads%') 
-- left join (
--   select
--   test_group,
--   minutes_bucket,
--   median(total_mins_prior) over (partition by test_group,minutes_bucket) median_minutes
--   from b
-- ) e on b.test_group=e.test_group and b.minutes_bucket=e.minutes_bucket 
group by 1,2
order by 1,2;


-- churn by quartile/minutes_bucket, LIMIT TO SCRIPPS VIEWERS, >=5M AD MINUTES
with users as (
select distinct user_id
from thunder_load.dwh_sonic_subscriptions_mv 
where report_date between '2021-04-10' and '2021-05-05' and 
(subs_product LIKE 'artemis-dtc-monthly-ads' or subs_product LIKE '%limited-ads%')  
group by 1
having sum(closing_active_ind)=26
),
diff_load as (
	select distinct a.ecid,test_group
	from digital_analytics.dplus_daily_content_streaming_user_level a
	join digital_analytics_dev.ad_load_test b on a.ecid=b.ecid and b.ecid is not null and 
		test_group like '%5m%' and
		nvl(total_ad_minutes,0)>=5 and 
		login_status='authenticated' and 
	  date between '2021-05-06' and '2021-08-17' and 
	  source_network in ('FOOD','DIY','COOK','TRAV','HGTV')
	union all 
	select distinct a.ecid,test_group
	from digital_analytics.dplus_daily_content_streaming_user_level a
	join digital_analytics_dev.ad_load_test b on a.ecid=b.ecid and b.ecid is not null and 
		test_group like '%4m%' and
		nvl(total_ad_minutes,0)>=4 and 
		login_status='authenticated' and 
	  date between '2021-05-06' and '2021-08-17' and 
	  source_network in ('FOOD','DIY','COOK','TRAV','HGTV')
),
a as (
select 
  test_group,a.user_id,
  sum(total_minutes)/26.0 total_mins_prior
from thunder_load.dwh_adobe_daily_on_demand_streaming_dtc a
join users b on a.user_id=b.user_id and nvl(ad_minutes,0)>0
join diff_load c on a.ecid=c.ecid
where 
  login_status='authenticated' and 
  date between '2021-04-10' and '2021-05-05'
group by 1,2
),
b as (
select 
  test_group,user_id,total_mins_prior,
  case when total_mins_prior<10 then '<10'
  when total_mins_prior between 10 and 30 then '10-30'
  when total_mins_prior between 30 and 60 then '30-60'
  when total_mins_prior between 60 and 100 then '60-100'
  when total_mins_prior >100 then '>100' end as minutes_bucket
  -- ntile(4) over (partition by a.test_group order by total_mins_prior desc) quartile
from a)
select
  b.test_group,
  b.minutes_bucket,
  -- median_minutes,
  count(distinct c.user_id) subs_active_0505,
  count(distinct d.user_id) subs_active_0627
from b 
join thunder_load.dwh_sonic_subscriptions_mv c 
on c.user_id=b.user_id  and closing_active_ind=1 and report_date='2021-05-05' and 
  (c.subs_product LIKE 'artemis-dtc-monthly-ads' or c.subs_product LIKE '%limited-ads%')  
left join thunder_load.dwh_sonic_subscriptions_mv_current d 
on c.user_id=d.user_id  and d.closing_active_ind=1 and 
  (d.subs_product LIKE 'artemis-dtc-monthly-ads' or d.subs_product LIKE '%limited-ads%') 
-- left join (
--   select
--   test_group,
--   minutes_bucket,
--   median(total_mins_prior) over (partition by test_group,minutes_bucket) median_minutes
--   from b
-- ) e on b.test_group=e.test_group and b.minutes_bucket=e.minutes_bucket 
group by 1,2
order by 1,2;


-- churn 4m vs. EVERYONE ELSE
with active_users as (
select distinct user_id
from thunder_load.dwh_sonic_subscriptions_mv 
where report_date between '2021-05-31' and '2021-07-04' and 
(subs_product LIKE 'artemis-dtc-monthly-ads' or subs_product LIKE '%limited-ads%')  
group by 1
having sum(closing_active_ind)=35
),
min_date as (
select user_id,min(CAST(DATEADD ('day',-1,DATEADD ('month',1,DATE_TRUNC('month',report_date))) AS DATE)) min_month
from thunder_load.dwh_sonic_subscriptions_mv
where user_id in (select user_id from active_users) and 
(subs_product LIKE 'artemis-dtc-monthly-ads' or subs_product LIKE '%limited-ads%')  
group by 1
),
four_ad_load as (
	select distinct a.user_id,test_group
	from digital_analytics.dplus_daily_content_streaming_user_level a
	join digital_analytics_dev.ad_load_test b on a.ecid=b.ecid and b.ecid is not null and 
		test_group like '%4m%' and
		login_status='authenticated' and 
	  date between '2021-05-31' and '2021-08-15'
),
diff_load as (
	select distinct a.user_id,
	case 
		when min_month='2021-01-31' then 'jan_5m_ads' 
		when min_month='2021-04-30' then 'apr_5m_ads'
		else 'Everyone else' 
		end as test_group
	from digital_analytics.dplus_daily_content_streaming_user_level a
	join 
		min_date b on a.user_id=b.user_id and 
		nvl(a.user_id,'null') not in (select nvl(user_id,'null') from four_ad_load) and
		login_status='authenticated' and 
	  date between '2021-05-31' and '2021-08-15'
	union all 
	select * from four_ad_load
),
a as (
select 
  test_group,a.user_id,
  sum(total_minutes)/35.0 total_mins_prior
from thunder_load.dwh_adobe_daily_on_demand_streaming_dtc a
join active_users b on a.user_id=b.user_id and nvl(ad_minutes,0)>0
join diff_load c on a.user_id=c.user_id
where 
  login_status='authenticated' and 
  date between '2021-05-31' and '2021-07-04'
group by 1,2
),
b as (
select 
  test_group,user_id,total_mins_prior,
  case when total_mins_prior<10 then '<10'
  when total_mins_prior between 10 and 30 then '10-30'
  when total_mins_prior between 30 and 80 then '30-80'
  when total_mins_prior >80 then '>80' end as minutes_bucket
from a)
select
  b.test_group,
  b.minutes_bucket,
  count(distinct c.user_id) subs_active_0704,
  count(distinct d.user_id) subs_active_0815
from b 
join thunder_load.dwh_sonic_subscriptions_mv c 
on c.user_id=b.user_id  and closing_active_ind=1 and report_date='2021-07-04' and 
  (c.subs_product LIKE 'artemis-dtc-monthly-ads' or c.subs_product LIKE '%limited-ads%')  
left join thunder_load.dwh_sonic_subscriptions_mv d 
on c.user_id=d.user_id  and d.closing_active_ind=1 and d.report_date='2021-08-15' and 
  (d.subs_product LIKE 'artemis-dtc-monthly-ads' or d.subs_product LIKE '%limited-ads%') 
group by 1,2
order by 1,2;

-- potential transfers
select 
	test_group,
	case when b.closing_active_ind=1 then 'closing_active' else 'not_closing_active' end as subs_status,
	case when b.subs_product LIKE '%ads%' then 'Ad-Lite' else 'Ad-Free' end as subs_active_plan,
	count(distinct c.user_id)
from digital_analytics_dev.ad_load_test a
join thunder_load.dwh_adobe_daily_on_demand_streaming_dtc c on a.ecid=c.ecid and a.ecid is not null and nvl(ad_minutes,0)>0 and date>='2021-05-06' 
join thunder_load.dwh_sonic_subscriptions_mv_current b on c.user_id=b.user_id  
join thunder_load.dwh_sonic_subscriptions_mv d on c.user_id=d.user_id and d.report_date='2021-05-05' and d.closing_active_ind=1
group by 1,2,3
order by 1,2;

-- potential transfers, LIMIT TO SCRIPPS VIEWERS, >=5M AD MINUTES
WITH diff_load as (
	select distinct a.ecid,test_group
	from digital_analytics.dplus_daily_content_streaming_user_level a
	join digital_analytics_dev.ad_load_test b on a.ecid=b.ecid and b.ecid is not null and 
		test_group like '%5m%' and
		nvl(total_ad_minutes,0)>=5 and 
		login_status='authenticated' and 
	  date between '2021-05-06' and '2021-08-17' and 
	  source_network in ('FOOD','DIY','COOK','TRAV','HGTV')
	union all 
	select distinct a.ecid,test_group
	from digital_analytics.dplus_daily_content_streaming_user_level a
	join digital_analytics_dev.ad_load_test b on a.ecid=b.ecid and b.ecid is not null and 
		test_group like '%4m%' and
		nvl(total_ad_minutes,0)>=4 and 
		login_status='authenticated' and 
	  date between '2021-05-06' and '2021-08-17' and 
	  source_network in ('FOOD','DIY','COOK','TRAV','HGTV')
)
select 
	test_group,
	case when b.closing_active_ind=1 then 'closing_active' else 'not_closing_active' end as subs_status,
	case when b.subs_product LIKE '%ads%' then 'Ad-Lite' else 'Ad-Free' end as subs_active_plan,
	count(distinct c.user_id)
from diff_load a
join thunder_load.dwh_adobe_daily_on_demand_streaming_dtc c on a.ecid=c.ecid and a.ecid is not null and nvl(ad_minutes,0)>0 and date>='2021-05-06' 
join thunder_load.dwh_sonic_subscriptions_mv_current b on c.user_id=b.user_id  
join thunder_load.dwh_sonic_subscriptions_mv d on c.user_id=d.user_id and d.report_date='2021-05-05' and d.closing_active_ind=1
group by 1,2,3
order by 1,2;

-- potential transfers,4m vs. EVERYONE ELSE
with active_users as (
select distinct user_id
from thunder_load.dwh_sonic_subscriptions_mv 
where report_date between '2021-05-31' and '2021-07-04' and 
(subs_product LIKE 'artemis-dtc-monthly-ads' or subs_product LIKE '%limited-ads%')  
group by 1
having sum(closing_active_ind)=35
),
min_date as (
select user_id,min(CAST(DATEADD ('day',-1,DATEADD ('month',1,DATE_TRUNC('month',report_date))) AS DATE)) min_month
from thunder_load.dwh_sonic_subscriptions_mv
where user_id in (select user_id from active_users) and 
(subs_product LIKE 'artemis-dtc-monthly-ads' or subs_product LIKE '%limited-ads%')  
group by 1
),
four_ad_load as (
	select distinct a.user_id,test_group
	from digital_analytics.dplus_daily_content_streaming_user_level a
	join digital_analytics_dev.ad_load_test b on a.ecid=b.ecid and b.ecid is not null and 
		test_group like '%4m%' and
		login_status='authenticated' and 
	  date between '2021-05-31' and '2021-08-15'
),
diff_load as (
	select distinct a.user_id,
	case 
		when min_month='2021-01-31' then 'jan_5m_ads' 
		when min_month='2021-04-30' then 'apr_5m_ads'
		else 'Everyone else' 
		end as test_group
	from digital_analytics.dplus_daily_content_streaming_user_level a
	join 
		min_date b on a.user_id=b.user_id and 
		nvl(a.user_id,'null') not in (select nvl(user_id,'null') from four_ad_load) and
		login_status='authenticated' and 
	  date between '2021-05-31' and '2021-08-15'
	union all 
	select * from four_ad_load
)
select 
	test_group,
	case when b.closing_active_ind=1 then 'closing_active' else 'not_closing_active' end as subs_status,
	case when b.subs_product LIKE '%ads%' then 'Ad-Lite' else 'Ad-Free' end as subs_active_plan,
	count(distinct c.user_id)
from diff_load a
join thunder_load.dwh_adobe_daily_on_demand_streaming_dtc c on a.user_id=c.user_id and a.user_id is not null and nvl(ad_minutes,0)>0 and date>='2021-07-04' 
join thunder_load.dwh_sonic_subscriptions_mv_current b on c.user_id=b.user_id  
join thunder_load.dwh_sonic_subscriptions_mv d on c.user_id=d.user_id and d.report_date='2021-07-04' and d.closing_active_ind=1
group by 1,2,3
order by 1,2;

-- engagement
with users as (
select distinct user_id
from thunder_load.dwh_sonic_subscriptions_mv
where report_date between '2021-04-10' and '2021-06-10' and 
(subs_product LIKE 'artemis-dtc-monthly-ads' or subs_product LIKE '%limited-ads%')	
group by 1
having sum(closing_active_ind)=62
),
a as (
select 
	test_group,c.user_id,
	-- platform,
	count(distinct a.ecid) ecid_prior,
	sum(ad_minutes)/26.0 ad_mins_prior,
	sum(content_minutes)/26.0 content_mins_prior,
	sum(total_minutes)/26.0 total_mins_prior
from thunder_load.dwh_adobe_daily_on_demand_streaming_dtc a
join digital_analytics_dev.ad_load_test b on a.ecid=b.ecid and b.ecid is not null and nvl(ad_minutes,0)>0
join users c on c.user_id=a.user_id 
where 
	login_status='authenticated' and 
	date between '2021-04-10' and '2021-05-05'
group by 1,2
-- ,3
),
b as (
select 
	test_group,c.user_id,
	-- platform,
	count(distinct a.ecid) ecid_after,
	sum(ad_minutes)/36.0 ad_mins_after,
	sum(content_minutes)/36.0 content_mins_after,
	sum(total_minutes)/36.0 total_mins_after
from thunder_load.dwh_adobe_daily_on_demand_streaming_dtc a
join digital_analytics_dev.ad_load_test b on a.ecid=b.ecid and b.ecid is not null and nvl(ad_minutes,0)>0
join users c on c.user_id=a.user_id 
where 
	login_status='authenticated' and 
	date between '2021-05-06' and '2021-06-10'
group by 1,2
-- ,3
),
c as (
select distinct
	a.test_group,
	a.user_id,
	-- a.platform,
	ecid_prior,ecid_after,
	ad_mins_prior,
	ad_mins_after,
	content_mins_prior,
	content_mins_after,
	total_mins_prior,
	total_mins_after
from a 
join b on a.test_group=b.test_group and a.user_id=b.user_id 
-- and a.platform=b.platform
)
select distinct
	test_group,
	-- platform,
	count(user_id) over (partition by test_group) subs_cnt,
	median(ad_mins_prior) over (partition by test_group) median_ad_mins_prior,
	median(ad_mins_after) over (partition by test_group) median_ad_mins_after,
	median(content_mins_prior) over (partition by test_group) median_content_mins_prior,
	median(content_mins_after) over (partition by test_group) median_content_mins_after,
	median(total_mins_prior) over (partition by test_group) median_total_mins_prior,
	median(total_mins_after) over (partition by test_group) median_total_mins_after,
	avg(ad_mins_prior) over (partition by test_group) avg_ad_mins_prior,
	avg(ad_mins_after) over (partition by test_group) avg_ad_mins_after,
	avg(content_mins_prior) over (partition by test_group) avg_content_mins_prior,
	avg(content_mins_after) over (partition by test_group) avg_content_mins_after,
	avg(total_mins_prior) over (partition by test_group) avg_total_mins_prior,
	avg(total_mins_after) over (partition by test_group) avg_total_mins_after
	-- ,median(ecid_prior) over (partition by test_group) median_ecid_prior,
	-- median(ecid_after) over (partition by test_group) median_ecid_after
from c
order by 1;


-- all control group with 4m_ads JAN
with users as (
select distinct a.user_id
from thunder_load.dwh_sonic_subscriptions_mv a
join (	
select distinct 	
	-- CAST(DATEADD ('day',-1,DATEADD ('month',1,DATE_TRUNC('month',report_date))) AS DATE) as month_ending,	
	user_id	
from thunder_load.dwh_sonic_subscriptions_mv	
	where 	
	new_acquisition_ind=1  AND 	
	(subs_product LIKE 'artemis-dtc-monthly-ads' or subs_product LIKE '%limited-ads%') and 	
	CAST(DATEADD ('day',-1,DATEADD ('month',1,DATE_TRUNC('month',report_date))) AS DATE) in ('2021-01-31')	
) b on a.user_id=b.user_id
where report_date between '2021-03-31' and '2021-06-10' and 
(subs_product LIKE 'artemis-dtc-monthly-ads' or subs_product LIKE '%limited-ads%')	
group by 1
having sum(closing_active_ind)=72
--limit 1000
)
,
a as (
select 
	'jan' month_joined,c.user_id,
	-- platform,
	count(distinct a.ecid) ecid_prior,
	sum(ad_minutes)/36.0 ad_mins_prior,
	sum(content_minutes)/36.0 content_mins_prior,
	sum(total_minutes)/36.0 total_mins_prior
from thunder_load.dwh_adobe_daily_on_demand_streaming_dtc a
-- join digital_analytics_dev.ad_load_test b on a.ecid<>b.ecid  and nvl(ad_minutes,0)>0
join users c on c.user_id=a.user_id 
where 
	login_status='authenticated' and nvl(ad_minutes,0)>0 and 
	date between '2021-03-31' and '2021-05-05'
	 and nvl(ecid,'00') not in (select distinct nvl(ecid,'00') from digital_analytics_dev.ad_load_test
	 where test_group like '%jan_5m_ads%'
	 )
group by 1,2

) 
,
b as (
select 
	'jan' month_joined,c.user_id,
	-- platform,
	count(distinct a.ecid) ecid_after,
	sum(ad_minutes)/36.0 ad_mins_after,
	sum(content_minutes)/36.0 content_mins_after,
	sum(total_minutes)/36.0 total_mins_after
from thunder_load.dwh_adobe_daily_on_demand_streaming_dtc a
-- join digital_analytics_dev.ad_load_test b on a.ecid<>b.ecid  and nvl(ad_minutes,0)>0
join users c on c.user_id=a.user_id 
where 
	login_status='authenticated' and nvl(ad_minutes,0)>0 and 
	date between '2021-05-06' and '2021-06-10' and  nvl(ecid,'00') not in (select distinct nvl(ecid,'00') from digital_analytics_dev.ad_load_test
	 where test_group like '%jan_5m_ads%'
	 )
group by 1,2
-- ,3
)
,
c as (
select distinct
	a.month_joined,
	a.user_id,
	-- a.platform,
	ecid_prior,ecid_after,
	ad_mins_prior,
	ad_mins_after,
	content_mins_prior,
	content_mins_after,
	total_mins_prior,
	total_mins_after
from a 
join b on a.month_joined=b.month_joined and a.user_id=b.user_id 
-- and a.platform=b.platform
)
select distinct
	month_joined,
	-- platform,
	count(user_id) over (partition by month_joined) subs_cnt,
	median(ad_mins_prior) over (partition by month_joined) median_ad_mins_prior,
	median(ad_mins_after) over (partition by month_joined) median_ad_mins_after,
	median(content_mins_prior) over (partition by month_joined) median_content_mins_prior,
	median(content_mins_after) over (partition by month_joined) median_content_mins_after,
	median(total_mins_prior) over (partition by month_joined) median_total_mins_prior,
	median(total_mins_after) over (partition by month_joined) median_total_mins_after,
	avg(ad_mins_prior) over (partition by month_joined) avg_ad_mins_prior,
	avg(ad_mins_after) over (partition by month_joined) avg_ad_mins_after,
	avg(content_mins_prior) over (partition by month_joined) avg_content_mins_prior,
	avg(content_mins_after) over (partition by month_joined) avg_content_mins_after,
	avg(total_mins_prior) over (partition by month_joined) avg_total_mins_prior,
	avg(total_mins_after) over (partition by month_joined) avg_total_mins_after
from c
order by 1;


-- all control group with 4m_ads APR
with users as (
select distinct a.user_id
from thunder_load.dwh_sonic_subscriptions_mv a
join (	
select distinct 	
	-- CAST(DATEADD ('day',-1,DATEADD ('month',1,DATE_TRUNC('month',report_date))) AS DATE) as month_ending,	
	user_id	
from thunder_load.dwh_sonic_subscriptions_mv	
	where 	
	new_acquisition_ind=1  AND 	
	(subs_product LIKE 'artemis-dtc-monthly-ads' or subs_product LIKE '%limited-ads%') and 	
	CAST(DATEADD ('day',-1,DATEADD ('month',1,DATE_TRUNC('month',report_date))) AS DATE) in ('2021-04-30')	
) b on a.user_id=b.user_id
where report_date between '2021-04-10' and '2021-06-30' and 
(subs_product LIKE 'artemis-dtc-monthly-ads' or subs_product LIKE '%limited-ads%')	
group by 1
having sum(closing_active_ind)=82
--limit 1000
)
,
a as (
select 
	'apr' month_joined,c.user_id,
	-- platform,
	count(distinct a.ecid) ecid_prior,
	sum(ad_minutes)/(1+date_diff('day',min(date),max(date))) ad_mins_prior,
	sum(content_minutes)/(1+date_diff('day',min(date),max(date))) content_mins_prior,
	sum(total_minutes)/(1+date_diff('day',min(date),max(date))) total_mins_prior
from thunder_load.dwh_adobe_daily_on_demand_streaming_dtc a
-- join digital_analytics_dev.ad_load_test b on a.ecid<>b.ecid  and nvl(ad_minutes,0)>0
join users c on c.user_id=a.user_id 
where 
	login_status='authenticated' and nvl(ad_minutes,0)>0 and 
	date between '2021-04-10' and '2021-05-05'
	 and nvl(ecid,'00') not in (select distinct nvl(ecid,'00') from digital_analytics_dev.ad_load_test
	 where test_group like '%apr_5m_ads%'
	 )
group by 1,2

) 
,
b as (
select 
	'apr' month_joined,c.user_id,
	-- platform,
	count(distinct a.ecid) ecid_after,
	sum(ad_minutes)/(1+date_diff('day',min(date),max(date))) ad_mins_after,
	sum(content_minutes)/(1+date_diff('day',min(date),max(date))) content_mins_after,
	sum(total_minutes)/(1+date_diff('day',min(date),max(date))) total_mins_after
from thunder_load.dwh_adobe_daily_on_demand_streaming_dtc a
-- join digital_analytics_dev.ad_load_test b on a.ecid<>b.ecid  and nvl(ad_minutes,0)>0
join users c on c.user_id=a.user_id 
where 
	login_status='authenticated' and nvl(ad_minutes,0)>0 and 
	date between '2021-05-06' and '2021-06-30' and  nvl(ecid,'00') not in (select distinct nvl(ecid,'00') from digital_analytics_dev.ad_load_test
	 where test_group like '%apr_5m_ads%'
	 )
group by 1,2
-- ,3
)
,
c as (
select distinct
	a.month_joined,
	a.user_id,
	-- a.platform,
	ecid_prior,ecid_after,
	ad_mins_prior,
	ad_mins_after,
	content_mins_prior,
	content_mins_after,
	total_mins_prior,
	total_mins_after
from a 
join b on a.month_joined=b.month_joined and a.user_id=b.user_id 
-- and a.platform=b.platform
)
select distinct
	month_joined,
	-- platform,
	count(user_id) over (partition by month_joined) subs_cnt,
	median(ad_mins_prior) over (partition by month_joined) median_ad_mins_prior,
	median(ad_mins_after) over (partition by month_joined) median_ad_mins_after,
	median(content_mins_prior) over (partition by month_joined) median_content_mins_prior,
	median(content_mins_after) over (partition by month_joined) median_content_mins_after,
	median(total_mins_prior) over (partition by month_joined) median_total_mins_prior,
	median(total_mins_after) over (partition by month_joined) median_total_mins_after
	-- avg(ad_mins_prior) over (partition by month_joined) avg_ad_mins_prior,
	-- avg(ad_mins_after) over (partition by month_joined) avg_ad_mins_after,
	-- avg(content_mins_prior) over (partition by month_joined) avg_content_mins_prior,
	-- avg(content_mins_after) over (partition by month_joined) avg_content_mins_after,
	-- avg(total_mins_prior) over (partition by month_joined) avg_total_mins_prior,
	-- avg(total_mins_after) over (partition by month_joined) avg_total_mins_after
from c
order by 1;

-- ENGAGEMENT BY QUARTILE/buckets
with users as (
select distinct user_id
from thunder_load.dwh_sonic_subscriptions_mv
where report_date between '2021-04-10' and '2021-08-03' and 
(subs_product LIKE 'artemis-dtc-monthly-ads' or subs_product LIKE '%limited-ads%')  
group by 1
having sum(closing_active_ind)=116
),
a as (
select 
  test_group,c.user_id,
  count(distinct a.ecid) ecid_prior,
  sum(ad_minutes)/26.0 ad_mins_prior,
  sum(content_minutes)/26.0 content_mins_prior,
  sum(total_minutes)/26.0 total_mins_prior
from thunder_load.dwh_adobe_daily_on_demand_streaming_dtc a
join digital_analytics_dev.ad_load_test b on a.ecid=b.ecid and b.ecid is not null and nvl(ad_minutes,0)>0
join users c on c.user_id=a.user_id 
where 
  login_status='authenticated' and 
  date between '2021-04-10' and '2021-05-05'
group by 1,2
),
b as (
select 
  test_group,c.user_id,
  count(distinct a.ecid) ecid_after,
  sum(ad_minutes)/56.0 ad_mins_after,
  sum(content_minutes)/56.0 content_mins_after,
  sum(total_minutes)/56.0 total_mins_after
from thunder_load.dwh_adobe_daily_on_demand_streaming_dtc a
join digital_analytics_dev.ad_load_test b on a.ecid=b.ecid and b.ecid is not null and nvl(ad_minutes,0)>0
join users c on c.user_id=a.user_id 
where 
  login_status='authenticated' and 
  date between '2021-05-06' and '2021-08-03'
group by 1,2
),
c as (
select distinct
  a.test_group,
  a.user_id,
  ecid_prior,ecid_after,
  ad_mins_prior,
  ad_mins_after,
  content_mins_prior,
  content_mins_after,
  total_mins_prior,
  total_mins_after,
  ntile(4) over (partition by a.test_group order by total_mins_prior desc)  quartile,
  case when total_mins_prior between 10 and 30 then '10-30'
  when total_mins_prior between 30 and 60 then '30-60'
  when total_mins_prior between 60 and 100 then '60-100'
  when total_mins_prior >100 then '>100' end as minutes_bucket
from a 
join b on a.test_group=b.test_group and a.user_id=b.user_id 
)
select distinct
  test_group,
  minutes_bucket,
  count(user_id) over (partition by test_group,minutes_bucket) subs_cnt,
  median(ad_mins_prior) over (partition by test_group,minutes_bucket) median_ad_mins_prior,
  median(ad_mins_after) over (partition by test_group,minutes_bucket) median_ad_mins_after,
  median(content_mins_prior) over (partition by test_group,minutes_bucket) median_content_mins_prior,
  median(content_mins_after) over (partition by test_group,minutes_bucket) median_content_mins_after,
  median(total_mins_prior) over (partition by test_group,minutes_bucket) median_total_mins_prior,
  median(total_mins_after) over (partition by test_group,minutes_bucket) median_total_mins_after,
  avg(ad_mins_prior) over (partition by test_group,minutes_bucket) avg_ad_mins_prior,
  avg(ad_mins_after) over (partition by test_group,minutes_bucket) avg_ad_mins_after,
  avg(content_mins_prior) over (partition by test_group,minutes_bucket) avg_content_mins_prior,
  avg(content_mins_after) over (partition by test_group,minutes_bucket) avg_content_mins_after,
  avg(total_mins_prior) over (partition by test_group,minutes_bucket) avg_total_mins_prior,
  avg(total_mins_after) over (partition by test_group,minutes_bucket) avg_total_mins_after
from c
union all 
select distinct
  test_group,
  quartile::varchar,
  count(user_id) over (partition by test_group,quartile) subs_cnt,
  median(ad_mins_prior) over (partition by test_group,quartile) median_ad_mins_prior,
  median(ad_mins_after) over (partition by test_group,quartile) median_ad_mins_after,
  median(content_mins_prior) over (partition by test_group,quartile) median_content_mins_prior,
  median(content_mins_after) over (partition by test_group,quartile) median_content_mins_after,
  median(total_mins_prior) over (partition by test_group,quartile) median_total_mins_prior,
  median(total_mins_after) over (partition by test_group,quartile) median_total_mins_after,
  avg(ad_mins_prior) over (partition by test_group,quartile) avg_ad_mins_prior,
  avg(ad_mins_after) over (partition by test_group,quartile) avg_ad_mins_after,
  avg(content_mins_prior) over (partition by test_group,quartile) avg_content_mins_prior,
  avg(content_mins_after) over (partition by test_group,quartile) avg_content_mins_after,
  avg(total_mins_prior) over (partition by test_group,quartile) avg_total_mins_prior,
  avg(total_mins_after) over (partition by test_group,quartile) avg_total_mins_after
from c
order by 1,2
;


-- ENGAGEMENT BY bucket, LIMIT TO SCRIPPS VIEWERS, >=5M AD MINUTES
with 
active_users as (
	select distinct user_id
	from thunder_load.dwh_sonic_subscriptions_mv
	where report_date between '2021-04-10' and '2021-08-17' and 
	(subs_product LIKE 'artemis-dtc-monthly-ads' or subs_product LIKE '%limited-ads%')  
	group by 1
	having sum(closing_active_ind)=130
),
diff_load as (
	select distinct a.ecid,test_group
	from digital_analytics.dplus_daily_content_streaming_user_level a
	join digital_analytics_dev.ad_load_test b on a.ecid=b.ecid and b.ecid is not null and 
		test_group like '%5m%' and
		nvl(total_ad_minutes,0)>=5 and 
		login_status='authenticated' and 
	  date between '2021-05-06' and '2021-08-17' and 
	  source_network in ('FOOD','DIY','COOK','TRAV','HGTV')
	union all 
	select distinct a.ecid,test_group
	from digital_analytics.dplus_daily_content_streaming_user_level a
	join digital_analytics_dev.ad_load_test b on a.ecid=b.ecid and b.ecid is not null and 
		test_group like '%4m%' and
		nvl(total_ad_minutes,0)>=4 and 
		login_status='authenticated' and 
	  date between '2021-05-06' and '2021-08-17' and 
	  source_network in ('FOOD','DIY','COOK','TRAV','HGTV')
),
a as (
select 
  test_group,c.user_id,
  count(distinct a.ecid) ecid_prior,
  sum(ad_minutes)/26.0 ad_mins_prior,
  sum(content_minutes)/26.0 content_mins_prior,
  sum(total_minutes)/26.0 total_mins_prior
from thunder_load.dwh_adobe_daily_on_demand_streaming_dtc a
join diff_load b on a.ecid=b.ecid and b.ecid is not null
join active_users c on c.user_id=a.user_id 
where 
  login_status='authenticated' and 
  date between '2021-04-10' and '2021-05-05'
group by 1,2
),
b as (
select 
  test_group,c.user_id,
  count(distinct a.ecid) ecid_after,
  sum(ad_minutes)/104.0 ad_mins_after,
  sum(content_minutes)/104.0 content_mins_after,
  sum(total_minutes)/104.0 total_mins_after
from thunder_load.dwh_adobe_daily_on_demand_streaming_dtc a
join diff_load b on a.ecid=b.ecid and b.ecid is not null
join active_users c on c.user_id=a.user_id 
where 
  login_status='authenticated' and 
  date between '2021-05-06' and '2021-08-17'
group by 1,2
),
c as (
select distinct
  a.test_group,
  a.user_id,
  ecid_prior,ecid_after,
  ad_mins_prior,
  ad_mins_after,
  content_mins_prior,
  content_mins_after,
  total_mins_prior,
  total_mins_after,
  ntile(4) over (partition by a.test_group order by total_mins_prior desc)  quartile,
  case when total_mins_prior between 10 and 30 then '10-30'
  when total_mins_prior between 30 and 60 then '30-60'
  when total_mins_prior between 60 and 100 then '60-100'
  when total_mins_prior >100 then '>100' end as minutes_bucket
from a 
join b on a.test_group=b.test_group and a.user_id=b.user_id 
)
select distinct
  test_group,
  minutes_bucket,
  count(user_id) over (partition by test_group,minutes_bucket) subs_cnt,
  median(ad_mins_prior) over (partition by test_group,minutes_bucket) median_ad_mins_prior,
  median(ad_mins_after) over (partition by test_group,minutes_bucket) median_ad_mins_after,
  median(content_mins_prior) over (partition by test_group,minutes_bucket) median_content_mins_prior,
  median(content_mins_after) over (partition by test_group,minutes_bucket) median_content_mins_after,
  median(total_mins_prior) over (partition by test_group,minutes_bucket) median_total_mins_prior,
  median(total_mins_after) over (partition by test_group,minutes_bucket) median_total_mins_after,
  avg(ad_mins_prior) over (partition by test_group,minutes_bucket) avg_ad_mins_prior,
  avg(ad_mins_after) over (partition by test_group,minutes_bucket) avg_ad_mins_after,
  avg(content_mins_prior) over (partition by test_group,minutes_bucket) avg_content_mins_prior,
  avg(content_mins_after) over (partition by test_group,minutes_bucket) avg_content_mins_after,
  avg(total_mins_prior) over (partition by test_group,minutes_bucket) avg_total_mins_prior,
  avg(total_mins_after) over (partition by test_group,minutes_bucket) avg_total_mins_after
from c
union all 
select distinct
  test_group,
  quartile::varchar,
  count(user_id) over (partition by test_group,quartile) subs_cnt,
  median(ad_mins_prior) over (partition by test_group,quartile) median_ad_mins_prior,
  median(ad_mins_after) over (partition by test_group,quartile) median_ad_mins_after,
  median(content_mins_prior) over (partition by test_group,quartile) median_content_mins_prior,
  median(content_mins_after) over (partition by test_group,quartile) median_content_mins_after,
  median(total_mins_prior) over (partition by test_group,quartile) median_total_mins_prior,
  median(total_mins_after) over (partition by test_group,quartile) median_total_mins_after,
  avg(ad_mins_prior) over (partition by test_group,quartile) avg_ad_mins_prior,
  avg(ad_mins_after) over (partition by test_group,quartile) avg_ad_mins_after,
  avg(content_mins_prior) over (partition by test_group,quartile) avg_content_mins_prior,
  avg(content_mins_after) over (partition by test_group,quartile) avg_content_mins_after,
  avg(total_mins_prior) over (partition by test_group,quartile) avg_total_mins_prior,
  avg(total_mins_after) over (partition by test_group,quartile) avg_total_mins_after
from c
order by 1,2
;


-- ENGAGEMENT BY bucket, 4m vs. EVERYONE ELSE, prior vs after
with 
active_users as (
	select distinct user_id
	from thunder_load.dwh_sonic_subscriptions_mv
	where report_date between '2021-05-31' and '2021-08-15' and 
	(subs_product LIKE 'artemis-dtc-monthly-ads' or subs_product LIKE '%limited-ads%')  
	group by 1
	having sum(closing_active_ind)=77
),
min_date as (
select user_id,min(CAST(DATEADD ('day',-1,DATEADD ('month',1,DATE_TRUNC('month',report_date))) AS DATE)) min_month
from thunder_load.dwh_sonic_subscriptions_mv
where user_id in (select user_id from active_users) and 
(subs_product LIKE 'artemis-dtc-monthly-ads' or subs_product LIKE '%limited-ads%') 
group by 1
),
four_ad_load as (
	select distinct a.user_id,test_group
	from digital_analytics.dplus_daily_content_streaming_user_level a
	join digital_analytics_dev.ad_load_test b on a.ecid=b.ecid and b.ecid is not null and 
		test_group like '%4m%' and
		login_status='authenticated' and 
	  date between '2021-05-31' and '2021-08-15'
),
diff_load as (
	select distinct a.user_id,
	case 
		when min_month='2021-01-31' then 'jan_5m_ads' 
		when min_month='2021-04-30' then 'apr_5m_ads'
		else 'Everyone else' 
		end as test_group
	from digital_analytics.dplus_daily_content_streaming_user_level a
	join 
		min_date b on a.user_id=b.user_id and 
		nvl(a.user_id,'null') not in (select nvl(user_id,'null') from four_ad_load) and
		login_status='authenticated' and 
	  date between '2021-05-31' and '2021-08-15'
	union all 
	select * from four_ad_load
),
a as (
select 
  test_group,c.user_id,
  count(distinct date)*1.0 viewing_days_prior,
  sum(total_minutes)/35.0 total_mins_prior
from thunder_load.dwh_adobe_daily_on_demand_streaming_dtc a
join diff_load b on a.ecid=b.ecid and b.ecid is not null
join active_users c on c.user_id=a.user_id 
where 
  login_status='authenticated' and 
  date between '2021-05-31' and '2021-07-04'
group by 1,2
),
b as (
select 
  test_group,c.user_id,
  count(distinct date)*1.0 viewing_days_after,
  sum(total_minutes)/35.0 total_mins_after
from thunder_load.dwh_adobe_daily_on_demand_streaming_dtc a
join diff_load b on a.ecid=b.ecid and b.ecid is not null
join active_users c on c.user_id=a.user_id 
where 
  login_status='authenticated' and 
  date between '2021-07-12' and '2021-08-15'
group by 1,2
),
c as (
select distinct
  a.test_group,
  a.user_id,
  viewing_days_prior,viewing_days_after,
  total_mins_prior,total_mins_after,
  case 
	when total_mins_prior<10 then '<10'
  when total_mins_prior between 10 and 30 then '10-30'
  when total_mins_prior between 30 and 60 then '30-60'
  when total_mins_prior between 60 and 100 then '60-100'
  when total_mins_prior >100 then '>100' end as minutes_bucket
from a 
join b on a.test_group=b.test_group and a.user_id=b.user_id 
)
select distinct
  test_group,
  minutes_bucket,
  count(user_id) over (partition by test_group,minutes_bucket) subs_cnt,
  median(viewing_days_prior) over (partition by test_group,minutes_bucket) median_viewing_days_prior,
  median(viewing_days_after) over (partition by test_group,minutes_bucket) median_viewing_days_after,
  median(total_mins_prior) over (partition by test_group,minutes_bucket) median_total_mins_prior,
  median(total_mins_after) over (partition by test_group,minutes_bucket) median_total_mins_after,
  avg(viewing_days_prior) over (partition by test_group,minutes_bucket) avg_viewing_days_prior,
  avg(viewing_days_after) over (partition by test_group,minutes_bucket) avg_viewing_days_after,
  avg(total_mins_prior) over (partition by test_group,minutes_bucket) avg_total_mins_prior,
  avg(total_mins_after) over (partition by test_group,minutes_bucket) avg_total_mins_after
from c
order by 1,2
;

-- weekly ENGAGEMENT BY bucket, 4m vs. EVERYONE ELSE
with 
active_users as (
	select distinct user_id
	from thunder_load.dwh_sonic_subscriptions_mv
	where report_date between '2021-05-24' and '2021-08-15' and 
	(subs_product LIKE 'artemis-dtc-monthly-ads' or subs_product LIKE '%limited-ads%')  
	group by 1
	having sum(closing_active_ind)=84
),
min_date as (
select user_id,min(CAST(DATEADD ('day',-1,DATEADD ('month',1,DATE_TRUNC('month',report_date))) AS DATE)) min_month
from thunder_load.dwh_sonic_subscriptions_mv
where user_id in (select user_id from active_users) and 
(subs_product LIKE 'artemis-dtc-monthly-ads' or subs_product LIKE '%limited-ads%')  
group by 1
),
four_ad_load as (
	select distinct a.user_id,test_group
	from digital_analytics.dplus_daily_content_streaming_user_level a
	join digital_analytics_dev.ad_load_test b on a.ecid=b.ecid and b.ecid is not null and 
		test_group like '%4m%' and
		login_status='authenticated' and 
	  date between '2021-05-24' and '2021-08-15'
),
diff_load as (
	select distinct a.user_id,
	case 
		when min_month='2021-01-31' then 'jan_5m_ads' 
		when min_month='2021-04-30' then 'apr_5m_ads'
		else 'Everyone else' 
		end as test_group
	from digital_analytics.dplus_daily_content_streaming_user_level a
	join 
		min_date b on a.user_id=b.user_id and 
		nvl(a.user_id,'null') not in (select nvl(user_id,'null') from four_ad_load) and
		login_status='authenticated' and 
	  date between '2021-05-24' and '2021-08-15'
	union all 
	select * from four_ad_load
),
user_level as (
	select 
	  test_group,c.user_id,end_of_week,
	  count(distinct date)*1.0 viewing_days,
	  sum(total_minutes) total_mins
	from thunder_load.dwh_adobe_daily_on_demand_streaming_dtc a
	join diff_load b on a.user_id=b.user_id and b.user_id is not null and nvl(ad_minutes,0)>0
	join active_users c on c.user_id=a.user_id 
	where 
	  login_status='authenticated' and 
	  date between '2021-05-24' and '2021-08-15'
	group by 1,2,3)
select distinct
	test_group,end_of_week,
	count(user_id)  over (partition by test_group,end_of_week) subs_count,
	avg(viewing_days) over (partition by test_group,end_of_week) avg_viewing_days,
	avg(total_mins) over (partition by test_group,end_of_week) avg_total_mins,
	median(viewing_days) over (partition by test_group,end_of_week) median_viewing_days,
	median(total_mins) over (partition by test_group,end_of_week) median_total_mins
from user_level;


-- weekly ENGAGEMENT BY bucket, 4m vs. EVERYONE ELSE, removed filters on subs and ad min > 0
with 
min_date as (
select user_id,min(CAST(DATEADD ('day',-1,DATEADD ('month',1,DATE_TRUNC('month',report_date))) AS DATE)) min_month
from thunder_load.dwh_sonic_subscriptions_mv
where 
(subs_product LIKE 'artemis-dtc-monthly-ads' or subs_product LIKE '%limited-ads%')  
group by 1
),
four_ad_load as (
	select distinct a.user_id,test_group
	from digital_analytics.dplus_daily_content_streaming_user_level a
	join digital_analytics_dev.ad_load_test b on a.ecid=b.ecid and b.ecid is not null and 
		test_group like '%4m%' and
		login_status='authenticated' and 
	  date between '2021-05-24' and '2021-08-15'
),
diff_load as (
	select distinct a.user_id,
	case 
		when min_month='2021-01-31' then 'jan_5m_ads' 
		when min_month='2021-04-30' then 'apr_5m_ads'
		else 'Everyone else' 
		end as test_group
	from digital_analytics.dplus_daily_content_streaming_user_level a
	join 
		min_date b on a.user_id=b.user_id and 
		nvl(a.user_id,'null') not in (select nvl(user_id,'null') from four_ad_load) and
		login_status='authenticated' and 
	  date between '2021-05-24' and '2021-08-15'
	union all 
	select * from four_ad_load
),
user_level as (
	select 
	  test_group,a.user_id,end_of_week,
	  count(distinct date)*1.0 viewing_days,
	  sum(total_minutes) total_mins
	from thunder_load.dwh_adobe_daily_on_demand_streaming_dtc a
	join diff_load b on a.user_id=b.user_id and b.user_id is not null 
	where 
	  login_status='authenticated' and 
	  date between '2021-05-24' and '2021-08-15'
	group by 1,2,3)
select distinct
	test_group,end_of_week,
	count(user_id)  over (partition by test_group,end_of_week) subs_count,
	avg(viewing_days) over (partition by test_group,end_of_week) avg_viewing_days,
	avg(total_mins) over (partition by test_group,end_of_week) avg_total_mins,
	median(viewing_days) over (partition by test_group,end_of_week) median_viewing_days,
	median(total_mins) over (partition by test_group,end_of_week) median_total_mins
from user_level;


-- % in each group
with 
min_date as (
select user_id,min(CAST(DATEADD ('day',-1,DATEADD ('month',1,DATE_TRUNC('month',report_date))) AS DATE)) min_month
from thunder_load.dwh_sonic_subscriptions_mv
where 
(subs_product LIKE 'artemis-dtc-monthly-ads' or subs_product LIKE '%limited-ads%')  
group by 1
),
four_ad_load as (
	select distinct a.user_id,test_group
	from digital_analytics.dplus_daily_content_streaming_user_level a
	join digital_analytics_dev.ad_load_test b on a.ecid=b.ecid and b.ecid is not null and 
		test_group like '%4m%' and
		login_status='authenticated' and 
	  date between '2021-08-16' and '2021-08-22'
),
diff_load as (
	select distinct a.user_id,
	case 
		when min_month='2021-01-31' then 'jan_5m_ads' 
		when min_month='2021-04-30' then 'apr_5m_ads'
		else 'Everyone else' 
		end as test_group
	from thunder_load.dwh_sonic_subscriptions_mv_current a
	join 
		min_date b on a.user_id=b.user_id and 
		a.closing_active_ind=1 and
		nvl(a.user_id,'null') not in (select nvl(user_id,'null') from four_ad_load)
	union all 
	select b.* 
	from four_ad_load b
	join thunder_load.dwh_sonic_subscriptions_mv_current a
	on a.user_id=b.user_id and 
		a.closing_active_ind=1 
)
select distinct
	test_group,
	count(user_id) 
from diff_load 
group by 1

