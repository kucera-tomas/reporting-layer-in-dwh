/*
File: analytics/cumulative_ltv.sql
Description: Calculates the Cumulative Lifetime Value (LTV) per customer.
             Tracks how much cash a customer has actually paid month-over-month.
*/
-- Calculate how much each customer spent in each specific month
WITH MonthlySpend AS (    
    SELECT 
        c.customer_id,
        c.company_name,
        DATE_FORMAT(t.tx_date, '%Y-%m-01') AS activity_month,
        
        SUM(s.amount) AS month_spend
    FROM stg_customers c
    INNER JOIN stg_subscriptions s 
        ON c.customer_id = s.customer_id
    INNER JOIN stg_transactions t 
        ON s.sub_id = t.sub_id
    WHERE LOWER(t.status) = 'success'
    GROUP BY 
        c.customer_id, 
        c.company_name, 
        DATE_FORMAT(t.tx_date, '%Y-%m-01')
)

-- Step 2: Use a Window Function to create the running total (Cumulative LTV)
SELECT 
    customer_id,
    company_name,
    activity_month,
    month_spend,
    
    -- The Magic: Sum the spend for this customer, ordered chronologically up to the current row's month
    SUM(month_spend) OVER (
        PARTITION BY customer_id 
        ORDER BY activity_month
    ) AS cumulative_ltv
    
FROM MonthlySpend
ORDER BY 
    customer_id, 
    activity_month;