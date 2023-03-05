# google-analytics-case-study

## Question 1
What were the number of events, unique sessions, and unique users (i.e., full visitors) that occurred in each hour? By “hour” let's assume we mean the hour in which the event occurred.

```sql
SELECT 
  hits.hour
  ,COUNT(*) as number_of_events
  ,COUNT(DISTINCT CONCAT(fullVisitorId, visitId)) as distinct_sessions
  ,COUNT(DISTINCT fullVisitorId) as distinct_visitors
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_20170801` 
,UNNEST(hits) AS hits
GROUP BY hits.hour
ORDER BY 1;
```
Using crossjoin on UNNEST(hits) in order to get one row per hit to calculate number of hits.

![plot](./img/question-1.png)

## Question 2
Each event or hit may be associated with nested product-related fields, found in hits.product. Let's suppose we want to know the top product categories (indicated by product.v2ProductCategory) with respect to the total number of unique users who either performed a “Quickview Click”, “Product Click”, or “Promotion Click” action (as indicated by hits.eventInfo.eventAction). We also want to make sure we're analyzing true user-actions (see hits.type) and not page views.


```sql
SELECT 
	product.v2ProductCategory
	,COUNT(DISTINCT fullVisitorId) as unique_users
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_20170801` 
,UNNEST(hits) as hits
,UNNEST(hits.product) as product
WHERE hits.type != 'PAGE'
AND hits.eventInfo.eventAction IN ('Quickview Click', 'Product Click', 'Promotion Click')
GROUP BY 1
ORDER BY 2 DESC;
```

![plot](./img/question-2.png)

## Question 3

```sql
SELECT 
	COALESCE(totals.transactions, 0) > 0 had_purchase
	,COUNT(DISTINCT CONCAT(fullVisitorId, visitId)) as distinct_sessions
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_20170801`
,UNNEST(hits) AS hits
WHERE hits.eventInfo.eventAction = 'Add to Cart'
GROUP BY 1;
```

![plot](./img/question-3.png)

## Question 4

```sql
WITH SESSIONS_OF_INTEREST AS (
  -- sessions with add to cart event
    SELECT 
    visitId
    ,fullVisitorId
    ,visitNumber
    ,COUNT(DISTINCT product.v2ProductName) as distinct_products_added_to_cart
    ,COUNT(*) as add_to_cart_events
    ,MIN(hits.time) as seconds_to_add_to_cart
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
  ,UNNEST(hits) AS hits
  ,UNNEST(hits.product) AS product
  WHERE hits.eventInfo.eventAction = 'Add to Cart'
  GROUP BY 1,2,3
),

HISTORICAL_USER_FEATURES AS (
  SELECT
    a.fullVisitorId
    ,COALESCE(MAX(b.totals.transactions), 0) as has_purchased_before
    ,MAX(b.visitStartTime) as last_session_start_time
  FROM SESSIONS_OF_INTEREST as a
  LEFT JOIN `bigquery-public-data.google_analytics_sample.ga_sessions_*` as b
    ON a.fullVisitorId = b.fullVisitorId
    -- ensure we are not including 'future' data
    AND a.visitNumber > COALESCE(b.visitNumber,1)
  GROUP BY 1
)

SELECT
    COALESCE(c.totals.transactions, 0) > 0 had_purchase -- target
    ,a.visitId
    ,a.fullVisitorId
    -- engagement features
    ,a.visitNumber
    ,a.distinct_products_added_to_cart
    ,a. add_to_cart_events
    ,a.seconds_to_add_to_cart
    ,c.totals.sessionQualityDim
    ,c.totals.timeOnSite
    ,c.totals.pageviews
  	-- intention features
    ,c.trafficSource.isTrueDirect
    ,c.trafficSource.medium
    ,c.device.deviceCategory
    -- customer information
    ,c.socialEngagementType
    ,c.geoNetwork.country = 'United States' as is_domestic
    -- historical
    ,b.has_purchased_before
    ,c.visitStartTime - b.last_session_start_time as time_since_last_session

FROM SESSIONS_OF_INTEREST as a
LEFT JOIN HISTORICAL_USER_FEATURES as b
  ON a.fullVisitorId = b.fullVisitorId
-- total features
LEFT JOIN `bigquery-public-data.google_analytics_sample.ga_sessions_*` as c
  ON a.visitId = c.visitId
  AND a.fullVisitorId = c.fullVisitorId;
```
