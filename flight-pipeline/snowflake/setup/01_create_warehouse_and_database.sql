-- snowflake/setup/01_create_warehouse_and_database.sql
-- Run this in the Snowflake web UI worksheet

-- Step 1: Create a Virtual Warehouse (compute engine)
-- SIZE = X-SMALL is the smallest (cheapest) option — perfect for learning
-- AUTO_SUSPEND means it turns off after 60 seconds of inactivity (saves credits)
CREATE WAREHOUSE IF NOT EXISTS FLIGHT_WH
    WITH WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    COMMENT = 'Warehouse for the flight pipeline project';

-- Step 2: Create our main database
CREATE DATABASE IF NOT EXISTS FLIGHT_DB
    COMMENT = 'Flight data engineering pipeline database';

-- Step 3: Create three schemas — one per data layer
USE DATABASE FLIGHT_DB;

-- RAW: Exact copy of incoming data — never modified
CREATE SCHEMA IF NOT EXISTS RAW
    COMMENT = 'Raw ingested flight data — source of truth';

-- CLEAN: Validated, deduplicated, type-cast data  
CREATE SCHEMA IF NOT EXISTS CLEAN
    COMMENT = 'Cleaned and validated flight data';

-- ANALYTICS: Aggregated tables for business insights
CREATE SCHEMA IF NOT EXISTS ANALYTICS
    COMMENT = 'Business-ready analytics tables';

-- Step 4: Create a dedicated role and user (production best practice)
-- Even for your personal project, this teaches proper access control
CREATE ROLE IF NOT EXISTS FLIGHT_PIPELINE_ROLE;

GRANT USAGE ON WAREHOUSE FLIGHT_WH TO ROLE FLIGHT_PIPELINE_ROLE;
GRANT ALL PRIVILEGES ON DATABASE FLIGHT_DB TO ROLE FLIGHT_PIPELINE_ROLE;
GRANT ALL PRIVILEGES ON ALL SCHEMAS IN DATABASE FLIGHT_DB TO ROLE FLIGHT_PIPELINE_ROLE;

-- Step 5: Grant the role to yourself (replace YOUR_USERNAME)
GRANT ROLE FLIGHT_PIPELINE_ROLE TO USER YOUR_USERNAME;

-- Verify everything was created
SHOW WAREHOUSES;
SHOW DATABASES;
SHOW SCHEMAS IN DATABASE FLIGHT_DB;