copy digital_analytics_dev.maru_cord_status 
from 's3://dci-prod-dataanalytics-teams-datastrategy-us-east-1/hdeng/maru/maru_cord_status.csv' 
iam_role 'arn:aws:iam::246607762912:role/prod-us-analytics-redshift-teams'  csv
EMPTYASNULL
IGNOREHEADER 1;

select count(distinct user_id)
from digital_analytics_dev.maru_cord_status;


drop table if exists #minutes;
create table #minutes as 
select 
CAST(DATEADD ('day',-1,DATEADD ('month',1,DATE_TRUNC('month',date))) AS DATE) AS end_of_month,
a.user_id active_user_id, 
cord_status,
sum(tve_total_stream_starts) + sum(tve_total_minutes) as viewing, 
sum(tve_total_minutes)/60.0 hours_watched,
count(distinct case when tve_total_stream_starts>0 or tve_total_minutes>0 then date end) viewing_days
from thunder_load.dwh_adobe_daily_user_summary_dtc a 
join digital_analytics_dev.survey_response b
on a.user_id=b.user_id
where 
  login_status = 'authenticated' and
  date between '2021-01-03' and '2021-07-31'
group by 1,2,3;

drop table if exists #subs_data;
create table #subs_data as 
WITH
capabilities AS
(
  SELECT *,
         CASE
           WHEN CAST(created_timestamp AS DATE) = batch_date THEN 1
           ELSE 0
         END AS new_caps_ind,
         CASE
           WHEN rn_user_id = 1 AND new_caps_ind = 1 THEN 1
           ELSE 0
         END AS new_acquisition_ind,
         CASE
           WHEN rn_user_id > 1 AND new_caps_ind = 1 AND (gap_from_prev_capability > 0) THEN 1
           ELSE 0
         END AS reinstate_ind,
         CASE
           WHEN rn_user_id > 1 AND new_caps_ind = 1 AND (gap_from_prev_capability IS NULL OR gap_from_prev_capability < 0) THEN 1
           ELSE 0
         END AS overlapping_reinstate_ind,
         new_acquisition_ind + reinstate_ind AS gross_add_ind,
         CASE
           WHEN CAST(capability_end_timestamp AS DATE) = batch_date THEN 1
           ELSE 0
         END AS new_terminated_ind,
         CASE
           WHEN new_terminated_ind = 1 AND (gap_to_next_capability IS NULL OR gap_to_next_capability > 0) THEN 1
           ELSE 0
         END AS churn_ind,
         CASE
           WHEN batch_date >= created_timestamp AND (capability_end_timestamp IS NULL OR batch_date <= capability_end_timestamp) THEN 1
           ELSE 0
         END AS opening_active_ind,
         CASE
           WHEN batch_date >= CAST(created_timestamp AS DATE) AND (capability_end_timestamp IS NULL OR batch_date < CAST(capability_end_timestamp AS DATE)) THEN 1
           ELSE 0
         END AS closing_active_ind,
         CASE
           WHEN (duration_months IS NOT NULL AND batch_date >= DATE_ADD('month',duration_months,CAST(capability_start_timestamp AS DATE))) THEN 1
           WHEN (next_capability_type = 'CONVERTED' AND batch_date >= CAST(next_capability_start_timestamp AS DATE)) THEN 1
           ELSE 0
         END AS free_trial_eligible_subs_ind,
         LAG(free_trial_eligible_subs_ind) OVER (PARTITION BY user_id,user_capability_id,capability_definition_id ORDER BY batch_date) AS free_trial_previously_eligible_ind,
         CASE
           WHEN free_trial_previously_eligible_ind = 1 THEN 0
           ELSE free_trial_eligible_subs_ind
         END AS free_trial_eligible_from_today_ind,
         CASE
           WHEN (capability_type = 'FREE_ON_PARTNER' AND new_terminated_ind = 1) AND (next_capability_type = 'CONVERTED' AND CAST(next_capability_start_timestamp AS DATE) = batch_date) THEN 1
           ELSE 0
         END free_trial_converted_today_ind,
         trunc(convert_timezone('America/New_York', capability_start_timestamp)) subs_created_date
  FROM (SELECT *,
               ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY batch_year,batch_month,batch_day,created_timestamp,capability_end_timestamp,user_capability_id) AS rn_user_id,
               LAG(capability_end_timestamp) OVER (PARTITION BY user_id,batch_year,batch_month,batch_day ORDER BY created_timestamp,capability_end_timestamp,user_capability_id) AS prev_capability_end_timestamp,
               DATE_DIFF('day',LAG(capability_end_timestamp) OVER (PARTITION BY user_id,batch_year,batch_month,batch_day ORDER BY created_timestamp,capability_end_timestamp,user_capability_id),created_timestamp) AS gap_from_prev_capability,
               LEAD(created_timestamp) OVER (PARTITION BY user_id,batch_year,batch_month,batch_day ORDER BY created_timestamp,capability_end_timestamp,user_capability_id) AS next_capability_start_timestamp,
               DATE_DIFF('day',capability_end_timestamp,LEAD(created_timestamp) OVER (PARTITION BY user_id,batch_year,batch_month,batch_day ORDER BY created_timestamp,capability_end_timestamp,user_capability_id)) AS gap_to_next_capability,
               LEAD(capability_type) OVER (PARTITION BY user_id,batch_year,batch_month,batch_day ORDER BY created_timestamp,capability_end_timestamp,user_capability_id) AS next_capability_type
        FROM (SELECT user_cap.batch_year,
                     user_cap.batch_month,
                     user_cap.batch_day,
                     CAST(user_cap.batch_year || '-' || user_cap.batch_month || '-' || user_cap.batch_day AS DATE) AS batch_date,
                     user_cap.user_id,
                     user_cap.realm,
                     user_cap.user_capability_id,
                     convert_timezone('America/New_York', user_cap.active_from) AS capability_start_timestamp,
                     convert_timezone('America/New_York', user_cap.active_to) AS capability_end_timestamp,
                     convert_timezone('America/New_York', user_cap.created_timestamp) AS created_timestamp,
                     user_cap.capability_definition_id,
                     def.name,
                     def.description,
                     cord_status,
                     CASE
                       WHEN capability_start_timestamp <= COALESCE(capability_end_timestamp,CAST('9999-12-31' AS DATE)) THEN 1
                       ELSE 0
                     END AS caps_valid_ind,
                     def.capability_type,
                     def.duration_months
              FROM thunder_load.dwh_sonic_user_capabilities AS user_cap
                JOIN (SELECT *,
                             CASE
                               WHEN capability_definition_id IN ('85b3c54d-4d4f-462f-a0c0-b08be4113e2a','c7cdf5d7-ac17-4bf7-b7cb-a0242712ee20','49194b79-1d42-46a6-a8bd-86abc5b93b2b') THEN 'FREE_ON_PARTNER'
                               WHEN capability_definition_id IN ('d6b76a24-1a2b-4b2a-a7c3-f33515064592','53d5841b-66bc-4dac-838f-d8f3c10e662c') THEN 'CONVERTED'
                               WHEN capability_definition_id IN ('ed9293c9-0b1c-4bfa-a930-45ee998d1174') THEN 'PAID'
                               ELSE 'OTHER'
                             END AS capability_type,
                             CASE
                               WHEN capability_definition_id = '85b3c54d-4d4f-462f-a0c0-b08be4113e2a' THEN 3
                               WHEN capability_definition_id = 'c7cdf5d7-ac17-4bf7-b7cb-a0242712ee20' THEN 6
                               WHEN capability_definition_id = '49194b79-1d42-46a6-a8bd-86abc5b93b2b' THEN 12
                             END AS duration_months
                      FROM thunder_load.dwh_sonic_capability_definitions_current
                      WHERE name LIKE '%Vz%'
                      OR    capability_definition_id IN ('49194b79-1d42-46a6-a8bd-86abc5b93b2b','85b3c54d-4d4f-462f-a0c0-b08be4113e2a','c7cdf5d7-ac17-4bf7-b7cb-a0242712ee20','53d5841b-66bc-4dac-838f-d8f3c10e662c','ed9293c9-0b1c-4bfa-a930-45ee998d1174','d6b76a24-1a2b-4b2a-a7c3-f33515064592')) AS def ON user_cap.capability_definition_id = def.capability_definition_id
              join digital_analytics_dev.survey_response b
              on user_cap.user_id=b.user_id
              WHERE 
                trunc(convert_timezone('America/New_York', user_cap.active_from)) >= '2021-01-04' and
                batch_date between '2021-01-04' and '2021-07-31' and
                trunc(convert_timezone('America/New_York', created_timestamp)) >= '2021-01-04'
              )
        WHERE caps_valid_ind = 1
        AND   CAST(created_timestamp AS DATE) <= batch_date)
  ORDER BY user_id,
           batch_date,
           created_timestamp,
           capability_end_timestamp,
           user_capability_id
)
SELECT 
      batch_date,
       CASE
         WHEN source is null THEN 'Null'
         WHEN subs_campaign_id IN ('19','20') THEN 'GIFT'
         WHEN (source = 'IAP' OR subs_payment_provider = 'Roku') THEN subs_payment_provider
         ELSE 'WEB'
       END AS payment_provider,
       CASE
           WHEN (subs_payment_provider = 'Roku' AND CAST(CONVERT_TIMEZONE('America/New_York', subs_start_timestamp) AS DATE) < '2021-01-07' AND batch_date < '2021-01-11') THEN 'Free' -- Sonic issue
           WHEN subs_paid_ind = 1 OR subs_campaign_id IN ('19','20') THEN 'Paid'
           ELSE 'Free'
         END AS PAIDORFREE,
         CASE
           WHEN is_free_trial_success = 1 OR (subs_payment_provider = 'Roku' AND CAST(CONVERT_TIMEZONE('America/New_York', subs_start_timestamp) AS DATE) < '2021-01-07' AND batch_date < '2021-01-11') THEN 'Y' -- Sonic issue
           ELSE 'N'
          END AS subs_started_free_trial,
       CASE
         WHEN subs_product LIKE 'artemis-dtc-monthly-ads' or subs_product LIKE '%limited-ads%' THEN 'Discovery+ Limited Ads'
          WHEN subs_product LIKE 'monthlycf' or subs_product LIKE '%commercial-free%' THEN 'Discovery+ Commercial Free'
         ELSE 'Other'
       END AS sub_type,
       'DIRECT' AS source,
       user_id,
       closing_active_ind,
       gross_add_ind,
       new_acquisition_ind,
       reinstate_ind,
       CASE
          WHEN subs_campaign_redemption_code IS NOT NULL AND gross_add_ind = 1 THEN 1
          ELSE 0
          END AS redemption_count,
       yday_cancelled_subs_ind,
       cancelled_subs_ind,
       intraday_transfer_ind,
       ended_intraday_transfer_ind,
       churn_ind,
       churned_subs_ind,
       is_free_trial_success,
       free_trial_converted_today_ind,
       free_trial_eligible_from_today_ind,
       trunc(convert_timezone('America/New_York', subs_start_timestamp)) subs_created_date,
       free_trial_eligible_subs_ind,
      free_trial_converted_subs_ind,
      total_subs_ind,
      cord_status
FROM (SELECT a.*,
            cord_status,
             CAST(batch_year || '-' || batch_month || '-' || batch_day AS DATE) AS batch_date,
             NVL(LAG(cancelled_subs_ind,1) OVER (PARTITION BY subscription_id ORDER BY batch_year,batch_month,batch_day),0) AS yday_cancelled_subs_ind
      FROM thunder_load.dwh_sonic_subscriptions_mv a
      join digital_analytics_dev.survey_response b
      on a.user_id=b.user_id
      where 
        batch_date between '2021-01-03' and '2021-07-31' and
        trunc(convert_timezone('America/New_York',subs_start_timestamp)) >= '2021-01-03' and 
        subs_valid_ind=1)
UNION
SELECT 
        batch_date,
       'Verizon' AS payment_provider,
       'Paid' AS PAIDORFREE,
       'N' AS subs_started_free_trial,
       name AS subs_product,
       'VERIZON' AS source,
       user_id,
       closing_active_ind,
       gross_add_ind,
       new_acquisition_ind,
       reinstate_ind,
       CAST (NULL AS INTEGER) AS redemption_count,
       CAST (NULL AS INTEGER) AS yday_cancelled_subs_ind,
       CAST (NULL AS INTEGER) AS cancelled_subs_ind,
       CAST (NULL AS INTEGER) AS intraday_transfer_ind,
       CAST (NULL AS INTEGER) AS ended_intraday_transfer_ind,
       churn_ind,
       CAST (NULL AS INTEGER) AS churned_subs_ind,
       1 AS is_free_trial_success,
       free_trial_converted_today_ind,
       free_trial_eligible_from_today_ind,
       subs_created_date,
       free_trial_eligible_from_today_ind,
       free_trial_converted_today_ind,
       CAST (NULL AS INTEGER),
       cord_status
FROM capabilities;

-- overall metrics
WITH grain_month as (
SELECT distinct CAST(DATEADD ('day',-1,DATEADD ('month',1,DATE_TRUNC('month',batch_date))) AS DATE) AS end_of_month,
       payment_provider,
       PAIDORFREE,
       subs_started_free_trial,
       c.cord_status,
       case when sub_type like '%Limited Ads%' then 'Ad-Lite' else 'Ad-Free' end as sub_type,
       COUNT(DISTINCT (CASE WHEN closing_active_ind = 1 AND batch_date = end_of_month THEN a.user_id ELSE NULL END)) AS CLOSINGACTIVESUBSCRIBERS,
       COUNT(DISTINCT (CASE WHEN gross_add_ind = 1 THEN a.user_id ELSE NULL END)) AS GROSSADDS,
       COUNT(DISTINCT (CASE WHEN churn_ind = 1 THEN a.user_id ELSE NULL END)) AS churns,
       COUNT(DISTINCT a.user_id) AS subs,
       COUNT(DISTINCT (CASE WHEN subs_started_free_trial = 'Y' AND closing_active_ind = 1 AND gross_add_ind = 1 THEN a.user_id ELSE NULL END)) AS NEWFREETRIAL,
       COUNT(DISTINCT (CASE WHEN free_trial_eligible_subs_ind = 1 AND free_trial_converted_subs_ind = 1 AND free_trial_converted_today_ind=1 THEN a.user_id ELSE NULL END)) AS NEW_COHORT_CONVERTED_SUBS
FROM #subs_data c
join digital_analytics_dev.survey_response a 
on a.user_id=c.user_id
group by 1,2,3,4,5,6
UNION ALL 
SELECT distinct CAST(DATEADD ('day',-1,DATEADD ('month',1,DATE_TRUNC('month',batch_date))) AS DATE) AS end_of_month,
       payment_provider,
       PAIDORFREE,
       subs_started_free_trial,
       'TOTAL' cord_status,
       'TOTAL' sub_type,
       COUNT(DISTINCT (CASE WHEN closing_active_ind = 1 AND batch_date = end_of_month THEN a.user_id ELSE NULL END)) AS CLOSINGACTIVESUBSCRIBERS,
       COUNT(DISTINCT (CASE WHEN gross_add_ind = 1 THEN a.user_id ELSE NULL END)) AS GROSSADDS,
       COUNT(DISTINCT (CASE WHEN churn_ind = 1 THEN a.user_id ELSE NULL END)) AS churns,
       COUNT(DISTINCT a.user_id) AS subs,
       COUNT(DISTINCT (CASE WHEN subs_started_free_trial = 'Y' AND closing_active_ind = 1 AND gross_add_ind = 1 THEN a.user_id ELSE NULL END)) AS NEWFREETRIAL,
       COUNT(DISTINCT (CASE WHEN free_trial_eligible_subs_ind = 1 AND free_trial_converted_subs_ind = 1 AND free_trial_converted_today_ind=1 THEN a.user_id ELSE NULL END)) AS NEW_COHORT_CONVERTED_SUBS
FROM #subs_data c
join digital_analytics_dev.survey_response a 
on a.user_id=c.user_id
group by 1,2,3,4,5,6
UNION ALL 
SELECT distinct CAST(DATEADD ('day',-1,DATEADD ('month',1,DATE_TRUNC('month',batch_date))) AS DATE) AS end_of_month,
       payment_provider,
       PAIDORFREE,
       subs_started_free_trial,
       c.cord_status,
       'TOTAL' sub_type,
       COUNT(DISTINCT (CASE WHEN closing_active_ind = 1 AND batch_date = end_of_month THEN a.user_id ELSE NULL END)) AS CLOSINGACTIVESUBSCRIBERS,
       COUNT(DISTINCT (CASE WHEN gross_add_ind = 1 THEN a.user_id ELSE NULL END)) AS GROSSADDS,
       COUNT(DISTINCT (CASE WHEN churn_ind = 1 THEN a.user_id ELSE NULL END)) AS churns,
       COUNT(DISTINCT a.user_id) AS subs,
       COUNT(DISTINCT (CASE WHEN subs_started_free_trial = 'Y' AND closing_active_ind = 1 AND gross_add_ind = 1 THEN a.user_id ELSE NULL END)) AS NEWFREETRIAL,
       COUNT(DISTINCT (CASE WHEN free_trial_eligible_subs_ind = 1 AND free_trial_converted_subs_ind = 1 AND free_trial_converted_today_ind=1 THEN a.user_id ELSE NULL END)) AS NEW_COHORT_CONVERTED_SUBS
FROM #subs_data c
join digital_analytics_dev.survey_response a 
on a.user_id=c.user_id
group by 1,2,3,4,5,6),
consumption as (
  select distinct s.end_of_month,s.cord_status,
  case when sub_type like '%Limited Ads%' then 'Ad-Lite' else 'Ad-Free' end as sub_type,
  COUNT(DISTINCT s.user_id) eligible_subs, 
  COUNT(DISTINCT active_user_id) active_subs, 
  COUNT(DISTINCT case when viewing>0 then active_user_id end) AS viewing_subs,
  sum(hours_watched) total_hours_watched,
  sum(hours_watched)/COUNT(DISTINCT s.user_id) hours_watched_per_total_sub,
  avg(case when viewing>0 then hours_watched end) hours_watched_per_viewing_sub,
  avg(case when viewing>0 then viewing_days end) viewing_days_per_viewing_sub
  from (
    select distinct cord_status,sub_type,CAST(DATEADD ('day',-1,DATEADD ('month',1,DATE_TRUNC('month',batch_date))) AS DATE) AS end_of_month,user_id
    from #subs_data) s
  left join #minutes b
  on s.user_id=b.active_user_id and s.end_of_month=b.end_of_month 
  group by 1,2,3
  union all 
  select distinct s.end_of_month,s.cord_status,sub_type,
  COUNT(DISTINCT s.user_id) eligible_subs, 
  COUNT(DISTINCT active_user_id) active_subs, 
  COUNT(DISTINCT case when viewing>0 then active_user_id end) AS viewing_subs,
  sum(hours_watched) total_hours_watched,
  sum(hours_watched)/COUNT(DISTINCT s.user_id) hours_watched_per_total_sub,
  avg(case when viewing>0 then hours_watched end) hours_watched_per_viewing_sub,
  avg(case when viewing>0 then viewing_days end) viewing_days_per_viewing_sub
  from (
    select distinct cord_status,'TOTAL' sub_type,CAST(DATEADD ('day',-1,DATEADD ('month',1,DATE_TRUNC('month',batch_date))) AS DATE) AS end_of_month,user_id
    from #subs_data) s
  left join #minutes b
  on s.user_id=b.active_user_id and s.end_of_month=b.end_of_month 
  group by 1,2,3
  union all 
  select distinct s.end_of_month,s.cord_status,sub_type,
  COUNT(DISTINCT s.user_id) eligible_subs, 
  COUNT(DISTINCT active_user_id) active_subs, 
  COUNT(DISTINCT case when viewing>0 then active_user_id end) AS viewing_subs,
  sum(hours_watched) total_hours_watched,
  sum(hours_watched)/COUNT(DISTINCT s.user_id) hours_watched_per_total_sub,
  avg(case when viewing>0 then hours_watched end) hours_watched_per_viewing_sub,
  avg(case when viewing>0 then viewing_days end) viewing_days_per_viewing_sub
  from (
    select distinct 'TOTAL' cord_status,'TOTAL' sub_type,CAST(DATEADD ('day',-1,DATEADD ('month',1,DATE_TRUNC('month',batch_date))) AS DATE) AS end_of_month,user_id
    from #subs_data) s
  left join #minutes b
  on s.user_id=b.active_user_id and s.end_of_month=b.end_of_month 
  group by 1,2,3
)
SELECT distinct
  g.end_of_month as month,
  g.cord_status, 
  g.sub_type,
  sum(case when PAIDORFREE='Free' then grossadds else 0 end) over (partition by g.sub_type,g.end_of_month,g.cord_status) as free_trial_gross_adds,
  sum(case when PAIDORFREE='Paid' then grossadds else 0 end) over (partition by g.sub_type,g.end_of_month,g.cord_status) as paid_gross_adds,
  sum(case when PAIDORFREE='Free' then grossadds else 0 end) over (partition by g.sub_type,g.end_of_month,g.cord_status)+sum(case when PAIDORFREE='Paid' then grossadds else 0 end) over (partition by g.sub_type,g.end_of_month,g.cord_status) as total_gross_adds,
  sum(churns) over (partition by g.sub_type,g.end_of_month,g.cord_status) as churns,
  sum(case when PAIDORFREE='Paid' then subs else 0 end) over (partition by g.sub_type,g.end_of_month,g.cord_status) as paid_subs,
  active_subs  logged_in_subs,
  viewing_subs,eligible_subs, 
  total_hours_watched,hours_watched_per_total_sub,
  hours_watched_per_viewing_sub
FROM grain_month g 
left join consumption c 
on g.end_of_month=c.end_of_month and g.cord_status=c.cord_status and g.sub_type=c.sub_type
order by 1;



-- RTP
WITH free_trial_rtp as (
  SELECT distinct
  c.user_id user_id_a,c.cord_status,
  subs_created_date subs_created_date_a,
  min(case when free_trial_eligible_subs_ind=1 then CAST(DATEADD ('day',-1,DATEADD ('month',1,DATE_TRUNC('month',batch_date))) AS DATE) end) over (partition by c.user_id,payment_provider,subs_created_date)  as free_trial_end_month,
  min(case when free_trial_eligible_subs_ind=1 AND free_trial_converted_subs_ind = 1 then CAST(DATEADD ('day',-1,DATEADD ('month',1,DATE_TRUNC('month',batch_date))) AS DATE) end) over (partition by c.user_id,payment_provider,subs_created_date)  as rtp_month
  from #subs_data c
  join digital_analytics_dev.survey_response a 
  on a.user_id=c.user_id
)
SELECT 
  free_trial_end_month,
  c.cord_status,
  COUNT(DISTINCT CASE WHEN rtp_month = free_trial_end_month THEN user_id END) AS rtp_same_month,
  COUNT(DISTINCT CASE WHEN datediff('month', free_trial_end_month, rtp_month) = 1 THEN user_id END) AS rtp_1_month_after,
  COUNT(DISTINCT CASE WHEN datediff('month', free_trial_end_month, rtp_month) = 2 THEN user_id END) AS rtp_2_months_after,
  COUNT(DISTINCT CASE WHEN free_trial_eligible_subs_ind=1 AND CAST(DATEADD ('day',-1,DATEADD ('month',1,DATE_TRUNC('month',batch_date))) AS DATE) = free_trial_end_month THEN user_id END) AS eligible_to_rtp_same_month,
  COUNT(DISTINCT CASE WHEN free_trial_eligible_subs_ind=1 AND free_trial_converted_subs_ind = 0 AND closing_active_ind = 1 AND CAST(DATEADD ('day',-1,DATEADD ('month',1,DATE_TRUNC('month',batch_date))) AS DATE) = free_trial_end_month THEN user_id END) AS grace_period_same_month,
  COUNT(DISTINCT CASE WHEN free_trial_eligible_subs_ind = 1 AND free_trial_converted_subs_ind = 0 AND closing_active_ind = 1 and datediff('month', free_trial_end_month, CAST(DATEADD ('day',-1,DATEADD ('month',1,DATE_TRUNC('month',batch_date))) AS DATE)) = 1 THEN user_id END) AS grace_period_1_month_after
FROM #subs_data c
join free_trial_rtp a 
on 
  c.user_id=user_id_a and 
  c.subs_created_date=subs_created_date_a
where free_trial_end_month is not null
group by 1,2
UNION ALL 
SELECT 
  free_trial_end_month,
  'TOTAL' cord_status,
  COUNT(DISTINCT CASE WHEN rtp_month = free_trial_end_month THEN user_id END) +
  COUNT(DISTINCT CASE WHEN datediff('month', free_trial_end_month, rtp_month) = 1 THEN user_id END) +
  COUNT(DISTINCT CASE WHEN datediff('month', free_trial_end_month, rtp_month) = 2 THEN user_id END) AS rtp,
  COUNT(DISTINCT CASE WHEN free_trial_eligible_subs_ind=1 AND CAST(DATEADD ('day',-1,DATEADD ('month',1,DATE_TRUNC('month',batch_date))) AS DATE) = free_trial_end_month THEN user_id END) AS eligible_to_rtp,
  COUNT(DISTINCT CASE WHEN free_trial_eligible_subs_ind=1 AND free_trial_converted_subs_ind = 0 AND closing_active_ind = 1 AND CAST(DATEADD ('day',-1,DATEADD ('month',1,DATE_TRUNC('month',batch_date))) AS DATE) = free_trial_end_month THEN user_id END) AS grace_period,
  rtp/eligible_to_rtp as 'RTP%',
  grace_period/eligible_to_rtp as 'grace_period%'
FROM #subs_data c
join free_trial_rtp a 
on 
  c.user_id=user_id_a and 
  c.subs_created_date=subs_created_date_a
where free_trial_end_month is not null
group by 1,2
order by 1 desc;

-- app brand consumption
with users as (
select 
  end_of_month,
  a.user_id viewing_user_id, 
  c.user_id as capable_user_id,
  c.cord_status,
  sonic_brand brand,
  sum(total_total_minutes)/60.0 hours_watched,
  count(distinct case when total_total_minutes>0 then date end)/days_capable::decimal(20,10) viewing_per_capable_days_ratio
from (
  select user_id,CAST(DATEADD ('day',-1,DATEADD ('month',1,DATE_TRUNC('month',batch_date))) AS DATE) AS end_of_month,cord_status,
  max(case when closing_active_ind=1 then batch_date end)-min(case when closing_active_ind=1 then batch_date end)+1 as days_capable
  from #subs_data 
  group by 1,2,3
  union all 
  select user_id,CAST(DATEADD ('day',-1,DATEADD ('month',1,DATE_TRUNC('month',batch_date))) AS DATE) AS end_of_month,'TOTAL',
  max(case when closing_active_ind=1 then batch_date end)-min(case when closing_active_ind=1 then batch_date end)+1 as days_capable
  from #subs_data 
  group by 1,2
) c 
join digital_analytics_dev.survey_response b
on c.user_id=b.user_id 
left join digital_analytics.dplus_daily_content_streaming_user_level a 
on 
  a.user_id=c.user_id and CAST(DATEADD ('day',-1,DATEADD ('month',1,DATE_TRUNC('month',date))) AS DATE)=end_of_month and 
  total_total_minutes > 0 and login_status = 'authenticated' and
  date between '2021-01-03' and '2021-07-31'
group by 1,2,3,4,5,days_capable)
select 
  a.end_of_month as month,a.cord_status,brand app_brand,
  count(distinct viewing_user_id)
  /capable_users::decimal(20,10) reach,
  count(distinct viewing_user_id) viewing_subs,
  sum(hours_watched) hours_watched,
  sum(hours_watched)/count(distinct viewing_user_id)::decimal(20,10) hours_watched_per_viewing_sub,
  avg(viewing_per_capable_days_ratio) avg_viewing_per_capable_days_ratio
from users a 
join (
  select end_of_month,cord_status,count(distinct capable_user_id) capable_users
  from users
  group by 1,2
) b 
on a.end_of_month=b.end_of_month and a.cord_status=b.cord_status
where brand is not null and brand!=''
group by 1,2,3,capable_users
order by 1,2,3,4 desc;

-- source brand consumption
with users as (
select 
  end_of_month,
  a.user_id viewing_user_id, 
  c.user_id as capable_user_id,
  c.cord_status,
  source_network source_brand,
  sum(total_total_minutes)/60.0 hours_watched,
  count(distinct case when total_total_minutes>0 then date end)/days_capable::decimal(20,10) viewing_per_capable_days_ratio
from (
  select user_id,CAST(DATEADD ('day',-1,DATEADD ('month',1,DATE_TRUNC('month',batch_date))) AS DATE) AS end_of_month,cord_status,
  max(case when closing_active_ind=1 then batch_date end)-min(case when closing_active_ind=1 then batch_date end)+1 as days_capable
  from #subs_data 
  group by 1,2,3
  union all 
  select user_id,CAST(DATEADD ('day',-1,DATEADD ('month',1,DATE_TRUNC('month',batch_date))) AS DATE) AS end_of_month,'TOTAL',
  max(case when closing_active_ind=1 then batch_date end)-min(case when closing_active_ind=1 then batch_date end)+1 as days_capable
  from #subs_data 
  group by 1,2
) c 
join digital_analytics_dev.survey_response b
on c.user_id=b.user_id 
left join digital_analytics.dplus_daily_content_streaming_user_level a 
on 
  a.user_id=c.user_id and CAST(DATEADD ('day',-1,DATEADD ('month',1,DATE_TRUNC('month',date))) AS DATE)=end_of_month and 
  total_total_minutes > 0 and login_status = 'authenticated' and
  date between '2021-01-03' and '2021-07-31'
group by 1,2,3,4,5,days_capable)
select 
  a.end_of_month as month,a.cord_status,source_brand,
  count(distinct viewing_user_id)
  /capable_users::decimal(20,10) reach,
  count(distinct viewing_user_id) viewing_subs,
  sum(hours_watched) hours_watched,
  sum(hours_watched)/count(distinct viewing_user_id)::decimal(20,10) hours_watched_per_viewing_sub,
  avg(viewing_per_capable_days_ratio) avg_viewing_per_capable_days_ratio
from users a 
join (
  select end_of_month,cord_status,count(distinct capable_user_id) capable_users
  from users
  group by 1,2
) b 
on a.end_of_month=b.end_of_month and a.cord_status=b.cord_status
where source_brand is not null and source_brand!=''
group by 1,2,3,capable_users
order by 1,2,3,4 desc;

-- consumption by genre
with users as (
select 
  end_of_month,
  a.user_id viewing_user_id, 
  c.user_id as capable_user_id,
  c.cord_status,
  primary_genre genre,
  sum(total_total_minutes)/60.0 hours_watched,
  count(distinct case when total_total_minutes>0 then date end)/days_capable::decimal(20,10) viewing_per_capable_days_ratio
from (
  select user_id,CAST(DATEADD ('day',-1,DATEADD ('month',1,DATE_TRUNC('month',batch_date))) AS DATE) AS end_of_month,cord_status,
  max(case when closing_active_ind=1 then batch_date end)-min(case when closing_active_ind=1 then batch_date end)+1 as days_capable
  from #subs_data 
  group by 1,2,3
  union all 
  select user_id,CAST(DATEADD ('day',-1,DATEADD ('month',1,DATE_TRUNC('month',batch_date))) AS DATE) AS end_of_month,'TOTAL',
  max(case when closing_active_ind=1 then batch_date end)-min(case when closing_active_ind=1 then batch_date end)+1 as days_capable
  from #subs_data 
  group by 1,2
) c 
join digital_analytics_dev.survey_response b
on c.user_id=b.user_id 
left join digital_analytics.dplus_daily_content_streaming_user_level a 
on 
  a.user_id=c.user_id and CAST(DATEADD ('day',-1,DATEADD ('month',1,DATE_TRUNC('month',date))) AS DATE)=end_of_month and 
  total_total_minutes > 0 and login_status = 'authenticated' and
  date between '2021-01-03' and '2021-07-31'
group by 1,2,3,4,5,days_capable)
select 
  a.end_of_month as month,a.cord_status,genre,
  count(distinct viewing_user_id)
  /capable_users::decimal(20,10) reach,
  count(distinct viewing_user_id) viewing_subs,
  sum(hours_watched) hours_watched,
  sum(hours_watched)/count(distinct viewing_user_id)::decimal(20,10) hours_watched_per_viewing_sub,
  avg(viewing_per_capable_days_ratio) avg_viewing_per_capable_days_ratio
from users a 
join (
  select end_of_month,cord_status,count(distinct capable_user_id) capable_users
  from users
  group by 1,2
) b 
on a.end_of_month=b.end_of_month and a.cord_status=b.cord_status
where genre is not null and genre!=''
group by 1,2,3,capable_users
order by 1,2,3,4 desc;

-- consumption by series
with users as (
select 
  end_of_month,
  a.user_id viewing_user_id, 
  c.user_id as capable_user_id,
  c.cord_status,
  sonic_series_name series_name,
  sum(total_total_minutes)/60.0 hours_watched,
  count(distinct case when total_total_minutes>0 then date end)/days_capable::decimal(20,10) viewing_per_capable_days_ratio
from (
  select user_id,CAST(DATEADD ('day',-1,DATEADD ('month',1,DATE_TRUNC('month',batch_date))) AS DATE) AS end_of_month,cord_status,
  max(case when closing_active_ind=1 then batch_date end)-min(case when closing_active_ind=1 then batch_date end)+1 as days_capable
  from #subs_data 
  group by 1,2,3
  union all 
  select user_id,CAST(DATEADD ('day',-1,DATEADD ('month',1,DATE_TRUNC('month',batch_date))) AS DATE) AS end_of_month,'TOTAL',
  max(case when closing_active_ind=1 then batch_date end)-min(case when closing_active_ind=1 then batch_date end)+1 as days_capable
  from #subs_data 
  group by 1,2
) c 
join digital_analytics_dev.survey_response b
on c.user_id=b.user_id 
left join digital_analytics.dplus_daily_content_streaming_user_level a 
on 
  a.user_id=c.user_id and CAST(DATEADD ('day',-1,DATEADD ('month',1,DATE_TRUNC('month',date))) AS DATE)=end_of_month and 
  total_total_minutes > 0 and login_status = 'authenticated' and
  date between '2021-01-03' and '2021-07-31'
group by 1,2,3,4,5,days_capable)
select 
  a.end_of_month as month,a.cord_status,series_name,
  count(distinct viewing_user_id)
  /capable_users::decimal(20,10) reach,
  count(distinct viewing_user_id) viewing_subs,
  sum(hours_watched) hours_watched,
  sum(hours_watched)/count(distinct viewing_user_id)::decimal(20,10) hours_watched_per_viewing_sub,
  avg(viewing_per_capable_days_ratio) avg_viewing_per_capable_days_ratio
from users a 
join (
  select end_of_month,cord_status,count(distinct capable_user_id) capable_users
  from users
  group by 1,2
) b 
on a.end_of_month=b.end_of_month and a.cord_status=b.cord_status
where series_name is not null
group by 1,2,3,capable_users
order by 1,2,3,4 desc;


-- genre/demo
with users as (
select 
  end_of_month,
  a.user_id viewing_user_id, 
  c.user_id as capable_user_id,
  cord_status,
  primary_genre genre,
  female,
  age::decimal(5,2),
  sum(total_total_minutes)/60.0 hours_watched,
  count(distinct case when total_total_minutes>0 then date end)/days_capable::decimal(20,10) viewing_per_capable_days_ratio
from (
  select user_id,CAST(DATEADD ('day',-1,DATEADD ('month',1,DATE_TRUNC('month',batch_date))) AS DATE) AS end_of_month,
  max(case when closing_active_ind=1 then batch_date end)-min(case when closing_active_ind=1 then batch_date end)+1 as days_capable
  from #subs_data 
  group by 1,2
) c 
join digital_analytics_dev.survey_response b
on c.user_id=b.user_id 
left join digital_analytics.dplus_daily_content_streaming_user_level a 
on 
  a.user_id=c.user_id and CAST(DATEADD ('day',-1,DATEADD ('month',1,DATE_TRUNC('month',date))) AS DATE)=end_of_month and 
  total_total_minutes > 0 and login_status = 'authenticated' and
  date between '2021-01-03' and '2021-07-31'
left join digital_analytics_dev.cohort_segment_demo_data_vz_included d
on segment_name like 'total_subscribers' and d.user_id=b.user_id
group by 1,2,3,4,5,6,7,days_capable)
select 
  a.end_of_month as month,a.cord_status,a.genre,
  median_age,
  avg(female) perc_female,
  avg(age) avg_age,
  count(distinct viewing_user_id)
  /capable_users::decimal(20,10) reach,
  count(distinct viewing_user_id) viewing_subs,
  sum(hours_watched) hours_watched,
  sum(hours_watched)/count(distinct viewing_user_id)::decimal(20,10) hours_watched_per_viewing_sub,
  avg(viewing_per_capable_days_ratio) avg_viewing_per_capable_days_ratio
from users a 
join (
  select end_of_month,cord_status,count(distinct capable_user_id) capable_users
  from users
  group by 1,2
) b 
on a.end_of_month=b.end_of_month and a.cord_status=b.cord_status
join (
select 
  end_of_month as month,cord_status,genre,
  median(age) over (partition by end_of_month,cord_status,genre) median_age
from users
) c on a.end_of_month=c.month and a.cord_status=c.cord_status and a.genre=c.genre
where a.genre is not null
group by 1,2,3,4,capable_users
order by 1,2,3;

-- consumption by show/genre/demo
with users as (
select 
  end_of_month,
  a.user_id viewing_user_id, 
  c.user_id as capable_user_id,
  d.user_id as demo_user_id,
  cord_status,
  sonic_series_name series_name,
  primary_genre genre,
  female::decimal(5,2),age::decimal(5,2),
  sum(total_total_minutes)/60.0 hours_watched,
  count(distinct case when total_total_minutes>0 then date end)/days_capable::decimal(20,10) viewing_per_capable_days_ratio
from (
  select user_id,CAST(DATEADD ('day',-1,DATEADD ('month',1,DATE_TRUNC('month',batch_date))) AS DATE) AS end_of_month,
  max(case when closing_active_ind=1 then batch_date end)-min(case when closing_active_ind=1 then batch_date end)+1 as days_capable
  from #subs_data 
  group by 1,2
) c 
join digital_analytics_dev.survey_response b
on c.user_id=b.user_id 
left join digital_analytics.dplus_daily_content_streaming_user_level a 
on 
  a.user_id=c.user_id and CAST(DATEADD ('day',-1,DATEADD ('month',1,DATE_TRUNC('month',date))) AS DATE)=end_of_month and 
  total_total_minutes > 0 and login_status = 'authenticated' and
  date between '2021-01-03' and '2021-07-31'
left join digital_analytics_dev.cohort_segment_demo_data_vz_included d
on segment_name like 'total_subscribers' and d.user_id=b.user_id
group by 1,2,3,4,5,6,7,8,9,days_capable)
select 
  a.end_of_month as month,a.cord_status,a.series_name,a.genre,
  avg(female) perc_female,
  avg(age) avg_age,
  median_age,
  count(distinct demo_user_id) demo_subs_matches,
  count(distinct viewing_user_id)
  /capable_users::decimal(20,10) reach,
  count(distinct viewing_user_id) viewing_subs,
  sum(hours_watched) hours_watched,
  sum(hours_watched)/count(distinct viewing_user_id)::decimal(20,10) hours_watched_per_viewing_sub,
  avg(viewing_per_capable_days_ratio) avg_viewing_per_capable_days_ratio
from users a 
join (
  select end_of_month,cord_status,count(distinct capable_user_id) capable_users
  from users
  group by 1,2
) b 
on a.end_of_month=b.end_of_month and a.cord_status=b.cord_status
join (
select distinct
  end_of_month as month,cord_status,series_name,genre,
  median(age) over (partition by end_of_month,cord_status,series_name,genre) median_age
from users
) c on a.end_of_month=c.month and a.cord_status=c.cord_status and a.series_name=c.series_name and a.genre=c.genre
where a.series_name is not null
group by 1,2,3,4,7,capable_users
order by 1,2,3;

-- consumption by genre/demo
with users as (
select 
  end_of_month,
  a.user_id viewing_user_id, 
  c.user_id as capable_user_id,
  d.user_id as demo_user_id,
  cord_status,
  primary_genre genre,
  female::decimal(5,2),age::decimal(5,2),
  sum(total_total_minutes)/60.0 hours_watched,
  count(distinct case when total_total_minutes>0 then date end)/days_capable::decimal(20,10) viewing_per_capable_days_ratio
from (
  select user_id,CAST(DATEADD ('day',-1,DATEADD ('month',1,DATE_TRUNC('month',batch_date))) AS DATE) AS end_of_month,
  max(case when closing_active_ind=1 then batch_date end)-min(case when closing_active_ind=1 then batch_date end)+1 as days_capable
  from #subs_data 
  group by 1,2
) c 
join digital_analytics_dev.survey_response b
on c.user_id=b.user_id 
left join digital_analytics.dplus_daily_content_streaming_user_level a 
on 
  a.user_id=c.user_id and CAST(DATEADD ('day',-1,DATEADD ('month',1,DATE_TRUNC('month',date))) AS DATE)=end_of_month and 
  total_total_minutes > 0 and login_status = 'authenticated' and
  date between '2021-01-03' and '2021-07-31'
left join digital_analytics_dev.cohort_segment_demo_data_vz_included d
on segment_name like 'total_subscribers' and d.user_id=b.user_id
group by 1,2,3,4,5,6,7,8,days_capable)
select 
  a.end_of_month as month,a.cord_status,
  a.genre,
  avg(female) perc_female,
  avg(age) avg_age,
  median_age,
  count(distinct demo_user_id) demo_subs_matches,
  count(distinct viewing_user_id)
  /capable_users::decimal(20,10) reach,
  count(distinct viewing_user_id) viewing_subs,
  sum(hours_watched) hours_watched,
  sum(hours_watched)/count(distinct viewing_user_id)::decimal(20,10) hours_watched_per_viewing_sub,
  avg(viewing_per_capable_days_ratio) avg_viewing_per_capable_days_ratio
from users a 
join (
  select end_of_month,cord_status,count(distinct capable_user_id) capable_users
  from users
  group by 1,2
) b 
on a.end_of_month=b.end_of_month and a.cord_status=b.cord_status
join (
select distinct
  end_of_month as month,cord_status,genre,
  median(age) over (partition by end_of_month,cord_status,genre) median_age
from users
) c on a.end_of_month=c.month and a.cord_status=c.cord_status and a.genre=c.genre
where a.genre is not null
group by 1,2,3,6,capable_users
order by reach desc;


#shark week/mag new subs contrib
select cord_status, count(distinct a.user_id)
from digital_analytics_dev.adobe_discovery_plus_daily_new_subscriber_share_by_asset a
join digital_analytics_dev.survey_response b 
on a.user_id=b.user_id and date>='2021-07-15'
join digital_analytics.sonic_content_metadata c 
on a.video_digital_id=c.video_id and source_network='MAG'
group by 1;

select cord_status, count(distinct a.user_id)
from digital_analytics_dev.adobe_discovery_plus_daily_new_subscriber_share_by_asset a
join digital_analytics_dev.survey_response b 
on a.user_id=b.user_id and date>='2021-07-11'
join digital_analytics.sonic_content_metadata c 
on a.video_digital_id=c.video_id and series_name in ('2-Headed Shark Attack','3-Headed Shark Attack','Air Jaws: Going for Gold','Brad Paisley''s Shark Country','Capsized: Blood in the Water','Crikey! It''s Shark Week','Deadliest Catch: Bloodline Shark Week','Dr. Pimple Popper Pops Shark Week','Envoy: Shark Cull','Expedition Unknown: Shark Trek','Extinct or Alive: Jaws of Alaska','Fin','Frenzy','Gordon Ramsay: Shark Bait','Great White Comeback','I Was Prey: Shark Week','I Was Prey: Terrors of the Deep','I was Prey: Shark Week 2021','Jackass Shark Week','Jaws Awakens: Phred vs. Slash','Jaws Awakens: Phred vs Slash','Josh Gates Tonight: Shark Week','Mech Shark','MechaShark','Mega Jaws of Bird Island','Mega Predators of Oz','Mega Shark vs Mecha Shark','Mega Shark vs. Giant Octopus','Megalodon','Monster Sharks of Andros Island','MotherSharker','Mothersharker','Mystery of the Black Demon Shark','Ninja Sharks 2: Mutants Rising','Raging Bulls','Return to Headstone Hell','Return to Shark Vortex','Return to the Lair of the Great White','Return to Lair of the Great White','Rogue Tiger Shark: The Hunt for Lagertha','Shark','Shark - Beneath the Surface','Shark Academy','Shark Rumble','Shark Week','Shark Week 2021','Shark Week Best in Show','Sharkadelic Summer 2','Sharkbait with David Dobrik','Sharknado','Sharknado 2: The Second One','Sharknado 3: Oh Hell No!','Sharknado 4: The 4th Awakens','Sharknado 5: Global Swarming','Spawn of El Diablo','Stranger Sharks','Submarine: Shark of Darkness','The Daily Bite','The Great Hammerhead Stakeout','The Great Shark Chase','The Last Sharknado: It''s About Time','The Real Sharknado','The Spawn Of El Diablo','Tiffany Haddish Does Shark Week','Tiger Queen','USS Indianapolis')
group by 1;

select cord_status, count(distinct a.user_id)
from digital_analytics_dev.sonic_discovery_plus_daily_subs_detail a
join digital_analytics_dev.survey_response b 
on a.user_id=b.user_id and date>='2021-07-11' and visitor_type='New'
group by 1

#shark week/mag consumption
with users as (
select 
  a.user_id viewing_user_id, 
  c.user_id as capable_user_id,
  b.cord_status,
  case when sonic_series_name in ('2-Headed Shark Attack','3-Headed Shark Attack','Air Jaws: Going for Gold','Brad Paisley''s Shark Country','Capsized: Blood in the Water','Crikey! It''s Shark Week','Deadliest Catch: Bloodline Shark Week','Dr. Pimple Popper Pops Shark Week','Envoy: Shark Cull','Expedition Unknown: Shark Trek','Extinct or Alive: Jaws of Alaska','Fin','Frenzy','Gordon Ramsay: Shark Bait','Great White Comeback','I Was Prey: Shark Week','I Was Prey: Terrors of the Deep','I was Prey: Shark Week 2021','Jackass Shark Week','Jaws Awakens: Phred vs. Slash','Jaws Awakens: Phred vs Slash','Josh Gates Tonight: Shark Week','Mech Shark','MechaShark','Mega Jaws of Bird Island','Mega Predators of Oz','Mega Shark vs Mecha Shark','Mega Shark vs. Giant Octopus','Megalodon','Monster Sharks of Andros Island','MotherSharker','Mothersharker','Mystery of the Black Demon Shark','Ninja Sharks 2: Mutants Rising','Raging Bulls','Return to Headstone Hell','Return to Shark Vortex','Return to the Lair of the Great White','Return to Lair of the Great White','Rogue Tiger Shark: The Hunt for Lagertha','Shark','Shark - Beneath the Surface','Shark Academy','Shark Rumble','Shark Week','Shark Week 2021','Shark Week Best in Show','Sharkadelic Summer 2','Sharkbait with David Dobrik','Sharknado','Sharknado 2: The Second One','Sharknado 3: Oh Hell No!','Sharknado 4: The 4th Awakens','Sharknado 5: Global Swarming','Spawn of El Diablo','Stranger Sharks','Submarine: Shark of Darkness','The Daily Bite','The Great Hammerhead Stakeout','The Great Shark Chase','The Last Sharknado: It''s About Time','The Real Sharknado','The Spawn Of El Diablo','Tiffany Haddish Does Shark Week','Tiger Queen','USS Indianapolis')
  then 'Shark Week'
  when source_network='MAG' then 'Magnolia'
  end as content_type,
  sum(total_total_minutes)/60.0 hours_watched
from #subs_data c 
join digital_analytics_dev.survey_response b
on c.user_id=b.user_id and c.closing_active_ind=1
left join digital_analytics.dplus_daily_content_streaming_user_level a 
on 
  a.user_id=c.user_id and 
  total_total_minutes > 0 and login_status = 'authenticated' and
  date >= '2021-07-15'
group by 1,2,3,4)
select 
  a.cord_status,
  content_type,
  count(distinct viewing_user_id)
  /capable_users::decimal(20,10) reach,
  count(distinct viewing_user_id) viewing_subs,
  sum(hours_watched) hours_watched,
  sum(hours_watched)/count(distinct viewing_user_id)::decimal(20,10) hours_watched_per_viewing_sub
from users a 
join (
  select cord_status,count(distinct capable_user_id) capable_users
  from users
  group by 1
) b 
on a.cord_status=b.cord_status
where a.content_type is not null
group by 1,2,capable_users
order by 1,2,3;

#reinstated
select distinct
a.ad_strategy subscription_type,'Direct' subscription_source,
CAST(DATEADD ('day',-1,DATEADD ('week',1,DATE_TRUNC('week',a.report_date))) AS DATE) week_ending_sun,
c.email_address,
'Reinstated' subscription_status
from thunder_load.dwh_sonic_subscriptions_mv a
join thunder_load.dwh_sonic_subscriptions_mv_current b on a.user_id=b.user_id and b.closing_active_ind=1
join thunder_load.dwh_blueshift_sonic_email_mapping c
on a.user_id=left(customer_id,46) and c.email_address is not null
where a.reinstate_ind=1 and a.subs_valid_ind=1
union all 
select distinct
'ad_free','Verizon',
CAST(DATEADD ('day',-1,DATEADD ('week',1,DATE_TRUNC('week',a.report_date))) AS DATE) week_ending_sun,
c.email_address,
'Reinstated'
from thunder_load.dwh_sonic_user_capabilities_mv a
join thunder_load.dwh_sonic_user_capabilities_mv_current b on a.user_id=b.user_id and b.closing_active_ind=1
join thunder_load.dwh_blueshift_sonic_email_mapping c
on a.user_id=left(customer_id,46) and c.email_address is not null
where a.reinstate_ind=1 and a.caps_valid_ind=1
;

-- naked & afraid, single life new subs contribution
with base as (
  select 
  cord_status, 
  case when series_name in ('Naked and Afraid') then 'Naked and Afraid'
  when series_name in ('Naked and Afraid of Love') then 'Naked and Afraid of Love'
  when series_name in ('90 Day: The Single Life') then '90 Day: The Single Life' end as series_name,
  cust_show_premiere_date,
  count(distinct a.user_id) new_subs_count_viewing_series
  from digital_analytics_dev.adobe_discovery_plus_daily_new_subscriber_share_by_asset a
  join digital_analytics_dev.survey_response b 
  on a.user_id=b.user_id and date>='2021-01-03'
  join digital_analytics.sonic_content_metadata c 
  on a.video_digital_id=c.video_id and 
  series_name in ('Naked and Afraid','Naked and Afraid of Love','90 Day: The Single Life')
  group by 1,2,3)
select distinct
b.cord_status,'Naked and Afraid' show_name, d.cust_show_premiere_date, new_subs_count_viewing_series, count(distinct a.user_id) total_new_viewing_subs
from digital_analytics_dev.adobe_discovery_plus_daily_new_subscriber_share_by_asset a
join digital_analytics_dev.survey_response b 
on a.user_id=b.user_id  
join base d on b.cord_status=d.cord_status and d.series_name='Naked and Afraid' and date>=cust_show_premiere_date
group by 1,2,3,4
union all 
select distinct
b.cord_status,'Naked and Afraid of Love' show_name, d.cust_show_premiere_date, new_subs_count_viewing_series, count(distinct a.user_id) total_new_viewing_subs
from digital_analytics_dev.adobe_discovery_plus_daily_new_subscriber_share_by_asset a
join digital_analytics_dev.survey_response b 
on a.user_id=b.user_id  
join base d on b.cord_status=d.cord_status and d.series_name='Naked and Afraid of Love' and date>=cust_show_premiere_date
group by 1,2,3,4
union all 
select distinct
b.cord_status,'90 Day: The Single Life' show_name, d.cust_show_premiere_date, new_subs_count_viewing_series, count(distinct a.user_id) total_new_viewing_subs
from digital_analytics_dev.adobe_discovery_plus_daily_new_subscriber_share_by_asset a
join digital_analytics_dev.survey_response b 
on a.user_id=b.user_id  
join base d on b.cord_status=d.cord_status and d.series_name='90 Day: The Single Life' and date>=cust_show_premiere_date
group by 1,2,3,4;
