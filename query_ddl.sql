-- create table
CREATE TABLE table_adidas (
    Retailer VARCHAR(50),
    Retailer_ID INT,
    Invoice_Date VARCHAR(50),
    Region VARCHAR(50),
    State VARCHAR(50),
    City VARCHAR(50),
    Product VARCHAR(50),
    Price_per_Unit VARCHAR(50),
    Units_Sold VARCHAR(50),
    Total_Sales VARCHAR(50),
    Operating_Profit VARCHAR(50),
    Operating_Margin VARCHAR(50),
    Sales_Method VARCHAR(50)
);

-- Import data dari file csv
COPY table_adidas
FROM '/tmp/dataset_raw.csv'
DELIMITER ','
CSV HEADER;