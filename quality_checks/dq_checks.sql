/*
File: quality_checks/dq_alerts.sql
Description: Audits the raw tables to catch anomalies based on business rules.
             Rows returned represent data quality issues that the ETL pipeline had to fix.
             (MariaDB/MySQL compatible version)
*/

-- ======================================================================
-- 1. CUSTOMER ALERTS
-- ======================================================================
SELECT customer_id 
FROM raw_customers 
WHERE customer_id < 0;

SELECT customer_id 
FROM raw_customers 
WHERE 
    NULLIF(TRIM(company_name), '') IS NULL 
 OR NULLIF(TRIM(country), '') IS NULL;

SELECT customer_id, signup_date 
FROM raw_customers 
WHERE CAST(signup_date AS DATE) > CURRENT_DATE;

-- ======================================================================
-- 2. SUBSCRIPTION ALERTS
-- ======================================================================
SELECT sub_id, amount
FROM raw_subscriptions 
WHERE amount < 0 OR amount IS NULL;

SELECT sub_id, start_date
FROM raw_subscriptions 
WHERE CAST(start_date AS DATE) > CURRENT_DATE;

SELECT sub_id, start_date, end_date
FROM raw_subscriptions 
WHERE CAST(end_date AS DATE) < CAST(start_date AS DATE);

SELECT sub_id, reference_id, customer_id
FROM raw_subscriptions 
WHERE customer_id NOT IN
(
    SELECT customer_id FROM raw_customers;
)

-- ======================================================================
-- 3. TRANSACTION ALERTS
-- ======================================================================
SELECT tx_id, status
FROM raw_transactions 
WHERE LOWER(status) NOT IN ('success', 'failed', 'refunded') 
      OR status IS NULL;

SELECT tx_id, tx_date
FROM raw_transactions 
WHERE CAST(tx_date AS DATE) > CURRENT_DATE;

SELECT tx_id, COUNT(*)
FROM raw_transactions 
GROUP BY tx_id 
HAVING COUNT(*) > 1;

SELECT tx_id, sub_id
FROM raw_transactions 
WHERE sub_id NOT IN 
(
    SELECT sub_id FROM raw_subscriptions
);