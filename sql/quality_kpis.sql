DELETE FROM quality_metrics WHERE metric_date = CURRENT_DATE();
DELETE FROM quality_scores_history WHERE report_date = CURRENT_DATE();

INSERT INTO quality_metrics (metric_date, pillar, column_name, score, issues_count, total_count, description)
SELECT 
    CURRENT_DATE(), 'Completude', 'Product_Name',
    (1 - (SUM(CASE WHEN Product_Name IS NULL THEN 1 ELSE 0 END) / COUNT(*))) * 100,
    SUM(CASE WHEN Product_Name IS NULL THEN 1 ELSE 0 END),
    COUNT(*),
    'Valeurs manquantes dans Product_Name'
FROM retail_raw;

INSERT INTO quality_metrics (metric_date, pillar, column_name, score, issues_count, total_count, description)
SELECT 
    CURRENT_DATE(), 'Completude', 'Unit_Price',
    (1 - (SUM(CASE WHEN Unit_Price IS NULL THEN 1 ELSE 0 END) / COUNT(*))) * 100,
    SUM(CASE WHEN Unit_Price IS NULL THEN 1 ELSE 0 END),
    COUNT(*),
    'Valeurs manquantes dans Unit_Price'
FROM retail_raw;

INSERT INTO quality_metrics (metric_date, pillar, column_name, score, issues_count, total_count, description)
SELECT 
    CURRENT_DATE(), 'Completude', 'Total_Amount',
    (1 - (SUM(CASE WHEN Total_Amount IS NULL THEN 1 ELSE 0 END) / COUNT(*))) * 100,
    SUM(CASE WHEN Total_Amount IS NULL THEN 1 ELSE 0 END),
    COUNT(*),
    'Valeurs manquantes dans Total_Amount'
FROM retail_raw;

INSERT INTO quality_metrics (metric_date, pillar, column_name, score, issues_count, total_count, description)
SELECT 
    CURRENT_DATE(), 'Exactitude', 'Customer_ID',
    (SUM(CASE WHEN Customer_ID REGEXP '^CUST-[0-9]{4}$' THEN 1 ELSE 0 END) / COUNT(*)) * 100,
    SUM(CASE WHEN Customer_ID NOT REGEXP '^CUST-[0-9]{4}$' THEN 1 ELSE 0 END),
    COUNT(*),
    'Format Customer_ID incorrect (attendu CUST-XXXX)'
FROM retail_raw;

INSERT INTO quality_metrics (metric_date, pillar, column_name, score, issues_count, total_count, description)
SELECT 
    CURRENT_DATE(), 'Validite', 'Unit_Price',
    (SUM(CASE WHEN Unit_Price > 0 THEN 1 ELSE 0 END) / COUNT(*)) * 100,
    SUM(CASE WHEN Unit_Price <= 0 OR Unit_Price IS NULL THEN 1 ELSE 0 END),
    COUNT(*),
    'Prix unitaire invalide (<= 0)'
FROM retail_raw;

INSERT INTO quality_metrics (metric_date, pillar, column_name, score, issues_count, total_count, description)
SELECT 
    CURRENT_DATE(), 'Validite', 'Quantity',
    (SUM(CASE WHEN Quantity > 0 THEN 1 ELSE 0 END) / COUNT(*)) * 100,
    SUM(CASE WHEN Quantity <= 0 OR Quantity IS NULL THEN 1 ELSE 0 END),
    COUNT(*),
    'Quantite invalide (<= 0)'
FROM retail_raw;

INSERT INTO quality_metrics (metric_date, pillar, column_name, score, issues_count, total_count, description)
SELECT 
    CURRENT_DATE(), 'Coherence', 'Total_Amount',
    (SUM(CASE WHEN ABS(Total_Amount - (Unit_Price * Quantity)) < 0.05 THEN 1 ELSE 0 END) / COUNT(*)) * 100,
    SUM(CASE WHEN ABS(Total_Amount - (Unit_Price * Quantity)) >= 0.05 OR Total_Amount IS NULL THEN 1 ELSE 0 END),
    COUNT(*),
    'Incoherence Total != Prix * Quantite'
FROM retail_raw;

INSERT INTO quality_metrics (metric_date, pillar, column_name, score, issues_count, total_count, description)
SELECT 
    CURRENT_DATE(), 'Unicite', 'Transaction_ID',
    (COUNT(DISTINCT Transaction_ID) / COUNT(*)) * 100,
    COUNT(*) - COUNT(DISTINCT Transaction_ID),
    COUNT(*),
    'Doublons de Transaction_ID'
FROM retail_raw;

INSERT INTO quality_metrics (metric_date, pillar, column_name, score, issues_count, total_count, description)
SELECT 
    CURRENT_DATE(), 'Actualite', 'Transaction_Date',
    (SUM(CASE WHEN Transaction_Date <= CURRENT_DATE() THEN 1 ELSE 0 END) / COUNT(*)) * 100,
    SUM(CASE WHEN Transaction_Date > CURRENT_DATE() OR Transaction_Date IS NULL THEN 1 ELSE 0 END),
    COUNT(*),
    'Date de transaction future ou invalide'
FROM retail_raw;

INSERT INTO quality_scores_history (report_date, completeness, accuracy, validity, consistency, uniqueness, timeliness, global_score)
SELECT 
    CURRENT_DATE(),
    (SELECT AVG(score) FROM quality_metrics WHERE pillar='Completude' AND metric_date=CURRENT_DATE()),
    (SELECT AVG(score) FROM quality_metrics WHERE pillar='Exactitude' AND metric_date=CURRENT_DATE()),
    (SELECT AVG(score) FROM quality_metrics WHERE pillar='Validite' AND metric_date=CURRENT_DATE()),
    (SELECT AVG(score) FROM quality_metrics WHERE pillar='Coherence' AND metric_date=CURRENT_DATE()),
    (SELECT AVG(score) FROM quality_metrics WHERE pillar='Unicite' AND metric_date=CURRENT_DATE()),
    (SELECT AVG(score) FROM quality_metrics WHERE pillar='Actualite' AND metric_date=CURRENT_DATE()),
    (SELECT AVG(score) FROM quality_metrics WHERE metric_date=CURRENT_DATE());
