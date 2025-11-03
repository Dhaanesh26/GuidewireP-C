-- Task 1

-- Postgres Table Definition

CREATE TABLE Inbound_file_data (
    Record_ID SERIAL PRIMARY KEY,        
    Customer_ID INT NOT NULL,            
    Customer_Name VARCHAR(100),
    Customer_Email VARCHAR(150),
    Amount DECIMAL(10,2),
    
    Orig_Source_File VARCHAR(255) NOT NULL,
    Updt_Source_File VARCHAR(255) NOT NULL,
    Created_On TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    Updated_On TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- adding unique constraint on Customer_ID for ON CONFLICT

ALTER TABLE Inbound_file_data
ADD CONSTRAINT uq_customer UNIQUE (Customer_ID);


-- SCENARIO 1

-- Drop if exists

DROP TABLE IF EXISTS staging_inbound;

-- create stagging table to match csv format (temporary table)

CREATE TABLE staging_inbound (
    Customer_ID INT,
    Customer_Name VARCHAR(100),
    Customer_Email VARCHAR(150),
    Amount DECIMAL(10,2)
);

-- Load the data Src_File_ 1.csv using import by right clicking staging_inbound

-- insert columns from staging_inbound into final table (Inbound_file_data)

INSERT INTO Inbound_file_data
(Customer_ID, Customer_Name, Customer_Email, Amount, Orig_Source_File, Updt_Source_File)
SELECT 
    Customer_ID, Customer_Name, Customer_Email, Amount,
    'Src_File_1.csv', 'Src_File_1.csv'
FROM staging_inbound
ON CONFLICT (Customer_ID)
DO UPDATE SET
    Customer_Name = EXCLUDED.Customer_Name,
    Customer_Email = EXCLUDED.Customer_Email,
    Amount = EXCLUDED.Amount,
    Updt_Source_File = EXCLUDED.Updt_Source_File,
    Updated_On = CURRENT_TIMESTAMP;

-- delete the table to load data same way from source file 2 and 3

TRUNCATE TABLE staging_inbound;

-- check for result

SELECT COUNT(*) AS total_records FROM Inbound_file_data;

-- column wise result

SELECT Record_ID, Customer_ID, Customer_Name, Orig_Source_File, Updt_Source_File, Created_On, Updated_On
FROM Inbound_file_data
ORDER BY Customer_ID;

-- SCENARIO 2

-- Load the data Src_File_ 2.csv using import by right clicking staging_inbound

-- Insert data into final table from stagging_inbound

INSERT INTO Inbound_file_data
(Customer_ID, Customer_Name, Customer_Email, Amount, Orig_Source_File, Updt_Source_File)
SELECT 
    Customer_ID, Customer_Name, Customer_Email, Amount,
    'Src_File_2.csv', 'Src_File_2.csv'
FROM staging_inbound
ON CONFLICT (Customer_ID)
DO UPDATE SET
    Customer_Name = EXCLUDED.Customer_Name,
    Customer_Email = EXCLUDED.Customer_Email,
    Amount = EXCLUDED.Amount,
    Updt_Source_File = EXCLUDED.Updt_Source_File,
    Updated_On = CURRENT_TIMESTAMP;

TRUNCATE TABLE staging_inbound;

-- SCENARIO 3

-- Load the data Src_File_ 3.csv using import by right clicking staging_inbound

-- Insert data into final table from stagging_inbound

INSERT INTO Inbound_file_data
(Customer_ID, Customer_Name, Customer_Email, Amount, Orig_Source_File, Updt_Source_File)
SELECT 
    Customer_ID, Customer_Name, Customer_Email, Amount,
    'Src_File_3.csv', 'Src_File_3.csv'
FROM staging_inbound
ON CONFLICT (Customer_ID)
DO UPDATE SET
    Customer_Name = EXCLUDED.Customer_Name,
    Customer_Email = EXCLUDED.Customer_Email,
    Amount = EXCLUDED.Amount,
    Updt_Source_File = EXCLUDED.Updt_Source_File,
    Updated_On = CURRENT_TIMESTAMP;


TRUNCATE TABLE staging_inbound;

SELECT COUNT(*) AS total_records FROM Inbound_file_data;

SELECT Record_ID, Customer_ID, Customer_Name, Orig_Source_File, Updt_Source_File, Created_On, Updated_On
FROM Inbound_file_data
ORDER BY Customer_ID
LIMIT 20;

-- Check specific Customer_IDs
SELECT *
FROM Inbound_file_data
WHERE Customer_ID IN (101,105,110,113,118,202,209,214);

