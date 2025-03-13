/*
=============================================================
Create Database and Schemas (PostgreSQL)
=============================================================
Script Purpose:
    This script creates a new database named 'datawarehouse' after checking if it already exists. 
    If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas 
    within the database: 'bronze', 'silver', and 'gold'.
    
WARNING:
    Running this script will drop the entire 'datawarehouse' database if it exists. 
    All data in the database will be permanently deleted. Proceed with caution 
    and ensure you have proper backups before running this script.
*/

-- Connect to the default PostgreSQL database
--\c postgres

-- Terminate existing connections and drop the 'datawarehouse' database if it exists
DO $$ 
BEGIN
    IF EXISTS (SELECT FROM pg_database WHERE datname = 'datawarehouse') THEN
        PERFORM pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = 'datawarehouse';
        EXECUTE 'DROP DATABASE datawarehouse';
    END IF;
END $$;
 
-- Create the 'datawarehouse' database
CREATE DATABASE datawarehouse;

-- Connect to the newly created database
--\c datawarehouse;

-- Create Schemas
CREATE SCHEMA bronze;
CREATE SCHEMA silver;
CREATE SCHEMA gold;
