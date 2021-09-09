index="events-api" version="2.*" type="interaction" subType="click" 
payload.productAttributes.name="dplus_us" payload.location="*Network Logo Rail*" payload.location="*home*"
| stats distinct_count(uuid) by session.sonicId session.platform payload.location
| stats count by session.platform payload.location
| fields session.platform payload.location
|rename payload.location as location |eval location=lower(location) | search location = "home|content-grid|*"


channel|content-grid|	tabbed-home|genres||

index="events-api" version="2.*" type="interaction"  payload.productAttributes.name="dplus_us"  
session.platform="firetv" payload.location="home|tabbed-content|*" 
| stats count by payload.location payload.targetText

-- BRAND HUBS
select distinct
    SUBSTRING(CAST(receivedtimestamp AT TIME ZONE 'America/New_York' AS VARCHAR), 1,7) as EST_month,
    cord_status,
    count(distinct json_extract_scalar(sessionjson, '$.sonicid')) user_count
FROM thunder_load.rawextract_thunder_events a 
join digital_analytics_workspace.cord_status b 
on json_extract_scalar(sessionjson, '$.sonicid')=user_id
and dt between '2021-03-01' and '2021-07-01'
AND version LIKE '2.%'
AND type = 'interaction'
AND payload.productattributes.name = 'dplus_us'
and subtype='click'
and session.platform='roku'
and  (lower(payload.location) like 'channel/%' or
     lower(payload.location) like 'channel|%')
-- and session.platform='firetv'
and session.flow!='anonymous'
group by 1,2
order by 1,2

-- GENRE PAGES
select distinct
    SUBSTRING(CAST(receivedtimestamp AT TIME ZONE 'America/New_York' AS VARCHAR), 1,7) as EST_month,
    cord_status,
    count(distinct json_extract_scalar(sessionjson, '$.sonicid')) user_count
FROM thunder_load.rawextract_thunder_events a 
join digital_analytics_workspace.cord_status b 
on json_extract_scalar(sessionjson, '$.sonicid')=user_id
and dt between '2021-03-01' and '2021-07-01'
AND version LIKE '2.%'
AND type = 'interaction'
AND payload.productattributes.name = 'dplus_us'
and subtype='click'
and session.platform='roku'
and payload.location = 'tabbed-home|genres||'
-- and session.platform='firetv'
-- and payload.location = 'home|tabbed-content|%'   
and payload.targetText!='For You'
and session.flow!='anonymous'
group by 1,2
order by 1,2
