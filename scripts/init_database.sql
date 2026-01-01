/*
 Purpose:
 --------
 Create Database and Schemas
 This script initializes the foundational environment for the Data Warehouse.
 It ensures a clean and repeatable setup by recreating the database and
 organizing data using layered schemas (Bronze, Silver, Gold).
 It forcefully drops and recreates the database if it already exists.
===============================================================================

WARNING:
    Running this script will drop the entire 'DataWarehouse' database if it exists. 
    All data in the database will be permanently deleted. Proceed with caution 
    and ensure you have proper backups before running this script.
*/

-- Switch to master to allow database-level operations
USE master;
GO

-- Check if DataWarehouse already exists (safe re-run)
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
	-- Force single connection to avoid drop failure due to active users
	ALTER DATABASE DataWarehouse
	SET SINGLE_USER
	WITH ROLLBACK IMMEDIATE;

	-- Drop existing database to start with a clean state
	DROP DATABASE DataWarehouse;
END;
GO

-- Create fresh DataWarehouse database
CREATE DATABASE DataWarehouse;
GO

-- Switch context to the new database
USE DataWarehouse;
GO

-- Bronze schema: stores raw, unprocessed source data(As-is)
CREATE SCHEMA bronze;
GO

-- Silver schema: stores cleaned and transformed data
CREATE SCHEMA silver;
GO

-- Gold schema: stores analytics-ready business data
CREATE SCHEMA gold;
GO
