import sweetviz as sv
import pandas as pd
import pymysql
import os

DB_HOST = "localhost"
DB_PORT = 3307
DB_USER = "dq_user"
DB_PASSWORD = "dq_password"
DB_NAME = "data_quality"

REPORT_DIR = r"c:\Users\idirt\OneDrive\Bureau\Mabrouk Hannachi\data-quality-pipeline\reports"
os.makedirs(REPORT_DIR, exist_ok=True)

def generate_reports():
    print("--- GÉNÉRATION RAPPORTS SWEETVIZ (RETAIL) ---")
    
    conn = pymysql.connect(host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASSWORD, database=DB_NAME)
    
    print("Loading retail_raw...")
    df_raw = pd.read_sql("SELECT * FROM retail_raw", conn)
    
    print("Loading retail_cleaned...")
    df_cleaned = pd.read_sql("SELECT * FROM retail_cleaned", conn)
    
    conn.close()
    
    print("Generating Raw Report...")
    df_raw['Total_Amount'] = df_raw['Total_Amount'].fillna(0)
    
    report_raw = sv.analyze(df_raw, target_feat='Total_Amount')
    raw_path = os.path.join(REPORT_DIR, "sweetviz_raw_report.html")
    report_raw.show_html(filepath=raw_path, open_browser=False)
    
    print("Generating Comparison Report...")
    report_compare = sv.compare([df_raw, "Raw Data"], [df_cleaned, "Cleaned Data"], target_feat='Total_Amount')
    compare_path = os.path.join(REPORT_DIR, "sweetviz_compare_report.html")
    report_compare.show_html(filepath=compare_path, open_browser=False)
    
    print(f"\nRapports générés :\n  - {raw_path}\n  - {compare_path}")

if __name__ == "__main__":
    generate_reports()
