/*
File: quality_checks/dq_alerts.sql
Description: Audits the raw tables to catch anomalies based on business rules.
             Rows returned represent data quality issues that the ETL pipeline had to fix.
             (MariaDB/MySQL compatible version)
*/

-- ======================================================================
-- 1. CUSTOMER ALERTS
-- ======================================================================
SELECT 
    'Negative Customer ID' AS issue_type, customer_id AS reference_id, CAST(customer_id AS CHAR) AS bad_value
FROM raw_customers 
WHERE customer_id < 0

UNION ALL

SELECT 
    'Missing Company or Country' AS issue_type, customer_id AS reference_id, 'Blank/Null' AS bad_value
FROM raw_customers 
WHERE NULLIF(TRIM(company_name), '') IS NULL OR NULLIF(TRIM(country), '') IS NULL

UNION ALL

SELECT 
    'Future Signup Date' AS issue_type, customer_id AS reference_id, CAST(signup_date AS CHAR) AS bad_value
FROM raw_customers 
WHERE CAST(signup_date AS DATE) > CURRENT_DATE;

-- ======================================================================
-- 2. SUBSCRIPTION ALERTS
-- ======================================================================
SELECT 
    'Negative or Null Amount' AS issue_type, sub_id AS reference_id, CAST(amount AS CHAR) AS bad_value
FROM raw_subscriptions 
WHERE amount < 0 OR amount IS NULL

UNION ALL

SELECT 
    'Future Start Date' AS issue_type, sub_id AS reference_id, CAST(start_date AS CHAR) AS bad_value
FROM raw_subscriptions 
WHERE CAST(start_date AS DATE) > CURRENT_DATE

UNION ALL

SELECT 
    'Inverted Dates (End < Start)' AS issue_type, sub_id AS reference_id, CONCAT('Start: ', start_date, ', End: ', end_date) AS bad_value
FROM raw_subscriptions 
WHERE CAST(end_date AS DATE) < CAST(start_date AS DATE)

UNION ALL

SELECT 
    'Subscription with Invalid Customer ID' AS issue_type, sub_id AS reference_id, CAST(customer_id AS CHAR) AS bad_value
FROM raw_subscriptions 
WHERE ABS(customer_id) NOT IN (SELECT ABS(customer_id) FROM raw_customers);

-- ======================================================================
-- 3. TRANSACTION ALERTS
-- ======================================================================
SELECT 
    'Invalid Status' AS issue_type, tx_id AS reference_id, status AS bad_value
FROM raw_transactions 
WHERE LOWER(status) NOT IN ('success', 'failed', 'refunded') OR status IS NULL

UNION ALL

SELECT 
    'Future Transaction Date' AS issue_type, tx_id AS reference_id, CAST(tx_date AS CHAR) AS bad_value
FROM raw_transactions 
WHERE CAST(tx_date AS DATE) > CURRENT_DATE

UNION ALL

SELECT 
    'Duplicate Transaction ID' AS issue_type, tx_id AS reference_id, CAST(COUNT(*) AS CHAR) AS bad_value
FROM raw_transactions 
GROUP BY tx_id 
HAVING COUNT(*) > 1

UNION ALL

SELECT 
    'Transaction with Invalid Subscription ID' AS issue_type, tx_id AS reference_id, CAST(sub_id AS CHAR) AS bad_value
FROM raw_transactions 
WHERE sub_id NOT IN (SELECT sub_id FROM raw_subscriptions);