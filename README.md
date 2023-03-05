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
![plot](./img/question-2.png)

## Question 3
![plot](./img/question-3.png)

## Question 4
