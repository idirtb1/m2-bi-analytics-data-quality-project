CREATE DATABASE IF NOT EXISTS data_quality;
USE data_quality;

DROP TABLE IF EXISTS retail_raw;
CREATE TABLE retail_raw (
    Transaction_ID INT,
    Customer_ID VARCHAR(50),
    Customer_Name VARCHAR(100),
    Product_Category VARCHAR(50),
    Product_Name VARCHAR(100),
    Unit_Price DECIMAL(10, 2),
    Quantity INT,
    Total_Amount DECIMAL(10, 2),
    Payment_Method VARCHAR(50),
    City VARCHAR(50),
    Transaction_Date DATE,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS retail_cleaned;
CREATE TABLE retail_cleaned (
    Transaction_ID INT PRIMARY KEY,
    Customer_ID VARCHAR(50),
    Customer_Name VARCHAR(100),
    Product_Category VARCHAR(50),
    Product_Name VARCHAR(100),
    Unit_Price DECIMAL(10, 2),
    Quantity INT,
    Total_Amount DECIMAL(10, 2),
    Payment_Method VARCHAR(50),
    City VARCHAR(50),
    Transaction_Date DATE
);

DROP TABLE IF EXISTS quality_metrics;
CREATE TABLE quality_metrics (
    id INT AUTO_INCREMENT PRIMARY KEY,
    metric_date DATE NOT NULL,
    pillar VARCHAR(50) NOT NULL,
    column_name VARCHAR(100),
    score DECIMAL(5, 2) NOT NULL,
    issues_count INT DEFAULT 0,
    total_count INT DEFAULT 0,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS quality_scores_history;
CREATE TABLE quality_scores_history (
    id INT AUTO_INCREMENT PRIMARY KEY,
    report_date DATE NOT NULL,
    completeness DECIMAL(5, 2),
    accuracy DECIMAL(5, 2),
    validity DECIMAL(5, 2),
    consistency DECIMAL(5, 2),
    uniqueness DECIMAL(5, 2),
    timeliness DECIMAL(5, 2),
    global_score DECIMAL(5, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DROP VIEW IF EXISTS quality_dashboard;
CREATE VIEW quality_dashboard AS
SELECT 
    report_date,
    completeness,
    accuracy,
    validity,
    consistency,
    uniqueness,
    timeliness,
    global_score,
    created_at
FROM quality_scores_history
ORDER BY report_date DESC
LIMIT 1;
