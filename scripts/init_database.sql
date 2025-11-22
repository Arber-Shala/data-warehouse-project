/*
==================================================================
Create Database and Schemas
==================================================================
Script Purpose:
	This script creates a new database called 'DataWarehouse' after checking if it already exists.
	If the database exists, it is dropped and recreated.
	Additionally, the script sets up three schemas within the database: 'bronze', 'silver', and 'gold'.

WARNING:
	Running this script will drop the entire 'datawarehouse' database if it exists.
	All data in 'datawarehouse' will be permanently deleted.
	Ensure you have proper backups before running this script.
*/


USE master;
GO

-- Drop and recreate the datawarehouse database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN 
	ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWarehouse;
END;
GO

-- Create the datawarehouse database
CREATE DATABASE DataWarehouse;
GO

USE DataWarehouse;
GO

CREATE SCHEMA bronze;
GO -- used as a separator when running multiple SQL queries at once
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO
