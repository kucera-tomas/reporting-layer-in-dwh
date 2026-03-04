/*
File: etl/stg_cleaning.sql
Description: Cleans row-level data quality issues (dates, negatives, nulls) 
             and deduplicates transactions.
*/

-- 1. Clean Customers
CREATE OR REPLACE VIEW stg_customers AS
SELECT
    -- Fix negative customer IDs
    ABS(customer_id) AS customer_id,
    
    -- Replace empty strings or nulls with a default value
    COALESCE(NULLIF(TRIM(company_name), ''), 'Unknown Company') AS company_name,
    COALESCE(NULLIF(TRIM(country), ''), 'Unknown Country') AS country,
    
    -- Cap future signup dates to today's date
    LEAST(CAST(signup_date AS DATE), CURRENT_DATE) AS signup_date
FROM raw_customers;


-- 2. Clean Subscriptions
CREATE OR REPLACE VIEW stg_subscriptions AS
SELECT
    sub_id,
    customer_id,
    plan_type,
    
    -- Fix negative subscription amounts
    ABS(COALESCE(amount, 0)) AS amount,
    
    -- Cap future start dates to today
    LEAST(CAST(start_date AS DATE), CURRENT_DATE) AS start_date,
    
    -- Fix inverted dates: if end_date is before start_date, assume it's an error and nullify it
    CASE 
        WHEN CAST(end_date AS DATE) < CAST(start_date AS DATE) THEN NULL 
        ELSE CAST(end_date AS DATE) 
    END AS end_date
FROM raw_subscriptions;


-- 3. Clean and Deduplicate Transactions
CREATE OR REPLACE VIEW stg_transactions AS
WITH CleanedTransactions AS (
    SELECT 
        tx_id,
        sub_id,
        
        -- Cap future transaction dates
        LEAST(CAST(tx_date AS DATE), CURRENT_DATE) AS tx_date,
        
        -- Handle unknown statuses, then uppercase them
        UPPER(
            CASE 
                WHEN LOWER(status) IN ('success', 'failed', 'refunded') THEN status
                ELSE 'Unknown'
            END
        ) AS status,
        
        -- Assign row numbers to handle duplicates
        ROW_NUMBER() OVER (
            PARTITION BY tx_id 
            ORDER BY tx_date DESC
        ) as row_num
    FROM raw_transactions
)
SELECT 
    tx_id,
    sub_id,
    tx_date,
    status
FROM CleanedTransactions
WHERE row_num = 1;


SELECT *
FROM stg_subscriptions;