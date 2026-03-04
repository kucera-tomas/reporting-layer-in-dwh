/*
File: etl/mart_sales_performance.sql
Description: Builds the final analytical view dm_sales_performance.
             One row per subscription, enriching with customer demographics,
             duration, and transaction success metrics.
*/

CREATE OR REPLACE VIEW dm_sales_performance AS
WITH SuccessfulPayments AS (
    SELECT sub_id, COUNT(tx_id) AS successful_payment_count
    FROM stg_transactions
    WHERE LOWER(status) = 'success'
    GROUP BY sub_id
)

SELECT 
    c.company_name AS customer_name,
    c.country,
    
    -- Calculate duration in days. If active, calculate up to today.
    DATEDIFF(
        COALESCE(s.end_date, CURRENT_DATE), 
        s.start_date
    ) AS duration_in_days,
    
    -- Set payments to 0 if payments doesn't exist
    COALESCE(p.successful_payment_count, 0) AS total_successful_payments

FROM stg_subscriptions s
INNER JOIN stg_customers c ON s.customer_id = c.customer_id
LEFT JOIN SuccessfulPayments p ON s.sub_id = p.sub_id;