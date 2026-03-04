/*
File: ddl/raw_schema.sql
Description: Table definitions for the raw landing zone. 
             Dates are set to VARCHAR to prevent load failures from malformed data.
*/

-- 1. Raw Customers Table
CREATE TABLE IF NOT EXISTS raw_customers (
    customer_id INT,
    company_name VARCHAR(255),
    country VARCHAR(100),
    signup_date VARCHAR(50)
);

-- 2. Raw Subscriptions Table
CREATE TABLE IF NOT EXISTS raw_subscriptions (
    sub_id INT,
    customer_id INT,
    plan_type VARCHAR(50),
    start_date VARCHAR(50),
    end_date VARCHAR(50),
    amount DECIMAL(10, 2)
);

-- 3. Raw Transactions Table
CREATE TABLE IF NOT EXISTS raw_transactions (
    tx_id INT,
    sub_id INT,
    tx_date VARCHAR(50),
    status VARCHAR(50)
);