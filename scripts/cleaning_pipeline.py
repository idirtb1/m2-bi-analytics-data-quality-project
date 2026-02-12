import pandas as pd
import numpy as np
import pymysql
import json
import os
from datetime import datetime

DB_CONFIG = {
    "host": "localhost",
    "port": 3307,
    "user": "dq_user",
    "password": "dq_password",
    "database": "data_quality"
}

REPORT_DIR = r"c:\Users\idirt\OneDrive\Bureau\Mabrouk Hannachi\data-quality-pipeline\reports"
DATA_DIR = r"c:\Users\idirt\OneDrive\Bureau\Mabrouk Hannachi\data-quality-pipeline\data\cleaned"
os.makedirs(REPORT_DIR, exist_ok=True)
os.makedirs(DATA_DIR, exist_ok=True)

def cleaning_pipeline():
    print("\n--- DÉMARRAGE DU PIPELINE DE NETTOYAGE ---\n")
    
    print("[1/4] Chargement des données brutes depuis MariaDB...")
    conn = pymysql.connect(**DB_CONFIG)
    query = "SELECT * FROM retail_raw"
    df = pd.read_sql(query, conn)
    print(f"  {len(df)} lignes chargées.")
    
    initial_count = len(df)
    cleaning_stats = {
        "initial_rows": initial_count,
        "steps": []
    }

    print("\n[A] Traitement de l'UNICITÉ...")
    dupes = df.duplicated().sum()
    df = df.drop_duplicates()
    print(f"  - Supprimé {dupes} doublons exacts.")
    cleaning_stats["steps"].append({"step": "deduplication_exact", "removed": int(dupes)})
    
    dupes_id = df.duplicated(subset=['Transaction_ID']).sum()
    df = df.drop_duplicates(subset=['Transaction_ID'], keep='first')
    print(f"  - Supprimé {dupes_id} doublons de Transaction_ID.")
    cleaning_stats["steps"].append({"step": "deduplication_id", "removed": int(dupes_id)})

    print("\n[B] Traitement de la COMPLÉTUDE...")
    nulls_pay = df['Payment_Method'].isnull().sum()
    df['Payment_Method'] = df['Payment_Method'].fillna('Unknown')
    print(f"  - Imputé {nulls_pay} 'Payment_Method' manquants avec 'Unknown'.")
    
    nulls_prod = df['Product_Name'].isnull().sum()
    df['Product_Name'] = df['Product_Name'].fillna('Product Unknown')
    print(f"  - Imputé {nulls_prod} 'Product_Name' manquants.")

    median_price = df['Unit_Price'].median()
    nulls_price = df['Unit_Price'].isnull().sum()
    df['Unit_Price'] = df['Unit_Price'].fillna(median_price)
    
    cols_qty_null = df['Quantity'].isnull().sum()
    df['Quantity'] = df['Quantity'].fillna(1)
    
    print(f"  - Imputé {nulls_price} prix (médiane: {median_price:.2f}) et {cols_qty_null} quantités (défaut: 1).")

    print("\n[C] Traitement de la VALIDITÉ...")
    neg_price = (df['Unit_Price'] < 0).sum()
    df['Unit_Price'] = df['Unit_Price'].abs()
    print(f"  - Corrigé {neg_price} prix négatifs (valeur absolue).")
    
    neg_qty = (df['Quantity'] <= 0).sum()
    df.loc[df['Quantity'] <= 0, 'Quantity'] = 1
    print(f"  - Corrigé {neg_qty} quantités négatives/nulles (forcé à 1).")

    print("\n[D] Traitement de la COHÉRENCE...")
    inconsistent = df[abs(df['Total_Amount'] - (df['Unit_Price'] * df['Quantity'])) > 0.05]
    count_inc = len(inconsistent)
    nulls_total = df['Total_Amount'].isnull().sum()
    
    df['Total_Amount'] = df['Unit_Price'] * df['Quantity']
    print(f"  - Recalculé {count_inc + nulls_total} montants totaux (incohérents ou manquants).")

    print("\n[E] Traitement de l'EXACTITUDE...")
    df['City'] = df['City'].str.strip().str.title()
    df['Customer_Name'] = df['Customer_Name'].str.strip().str.title()

    print("\n[3/4] Sauvegarde des données...")
    
    csv_path = os.path.join(DATA_DIR, "Retail_Cleaned.csv")
    df.to_csv(csv_path, index=False)
    print(f"  - CSV sauvegardé : {csv_path}")
    
    try:
        cursor = conn.cursor()
        cursor.execute("TRUNCATE TABLE retail_cleaned")
        
        cols = [
            "Transaction_ID", "Customer_ID", "Customer_Name", "Product_Category", 
            "Product_Name", "Unit_Price", "Quantity", "Total_Amount", 
            "Payment_Method", "City", "Transaction_Date"
        ]
        
        batch_size = 1000
        batch = []
        sql = f"INSERT INTO retail_cleaned ({', '.join(cols)}) VALUES ({', '.join(['%s']*len(cols))})"
        
        for _, row in df.iterrows():
            batch.append(tuple(row[c] for c in cols))
            if len(batch) >= batch_size:
                cursor.executemany(sql, batch)
                conn.commit()
                batch = []
        
        if batch:
            cursor.executemany(sql, batch)
            conn.commit()
            
        print(f"  - Table 'retail_cleaned' remplie : {len(df)} lignes.")
    except Exception as e:
        print(f"  ERREUR SQL : {e}")
    finally:
        conn.close()

    final_count = len(df)
    cleaning_stats["final_rows"] = final_count
    cleaning_stats["rows_removed"] = initial_count - final_count
    
    json_path = os.path.join(REPORT_DIR, "cleaning_report.json")
    with open(json_path, 'w') as f:
        json.dump(cleaning_stats, f, indent=4)
    print(f"  - Rapport JSON généré : {json_path}")

    print(f"\nsuccès : {initial_count} -> {final_count} lignes conservées.")

if __name__ == "__main__":
    cleaning_pipeline()
