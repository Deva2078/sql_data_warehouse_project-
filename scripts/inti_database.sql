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

-- Terminate existing connections and drop the 'datawarehouse' database if it exists
DO $$ 
DECLARE 
    db_exists BOOLEAN;
BEGIN
    -- Check if the database exists
    SELECT EXISTS (SELECT FROM pg_database WHERE datname = 'datawarehouse') INTO db_exists;

    IF db_exists THEN
        -- Terminate all active connections to the database
        PERFORM pg_terminate_backend(pg_stat_activity.pid)
        FROM pg_stat_activity
        WHERE pg_stat_activity.datname = 'datawarehouse' 
        AND pid <> pg_backend_pid();  -- Avoid killing the current session
        
        -- Drop the database
        EXECUTE 'DROP DATABASE datawarehouse';
    END IF;
END $$;

-- Create the 'datawarehouse' database
CREATE DATABASE datawarehouse;

-- connect to your created database by following syntax :
psql -U postgres -d datawarehouse

-- Create Schemas
CREATE SCHEMA bronze;
CREATE SCHEMA silver;
CREATE SCHEMA gold;
