/*
File: analytics/mrr_calculation.sql
Description: Calculates the Monthly Recurring Revenue (MRR) per customer.
             Tracks the revenue generated each month from active subscriptions.
*/

WITH RECURSIVE Calendar AS (
    SELECT DATE_FORMAT(MIN(start_date), '%Y-%m') AS report_month
    FROM raw_subscriptions

    UNION ALL

    SELECT DATE_ADD(report_month, INTERVAL 1 MONTH)
    FROM Calendar
    WHERE report_month <= DATE_FORMAT(CURRENT_DATE, '%Y-%m')
),
NormalizedSubscriptions AS (
    SELECT 
        CASE 
             WHEN plan_type = 'Annual' THEN amount / 12
             WHEN plan_type = 'Monthly' THEN amount
        ELSE 0 END AS monthly_cost,
        DATE_FORMAT(start_date, '%Y-%m') AS start_month,
        DATE_FORMAT(COALESCE(end_date, CURRENT_DATE), '%Y-%m') AS end_month
    FROM raw_transactions t
    INNER JOIN raw_subscriptions s ON t.sub_id = s.sub_id
    WHERE LOWER(t.status) = 'success'
)
SELECT report_month, SUM(monthly_cost) AS mrr
FROM Calendar c
LEFT JOIN NormalizedSubscriptions ns 
    ON c.report_month 
    BETWEEN ns.start_month 
    AND ns.end_month
GROUP BY report_month;