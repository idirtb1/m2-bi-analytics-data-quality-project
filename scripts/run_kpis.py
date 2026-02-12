import pymysql
import os

DB_HOST = "localhost"
DB_PORT = 3307
DB_USER = "dq_user"
DB_PASSWORD = "dq_password"
DB_NAME = "data_quality"

SQL_FILE = r"c:\Users\idirt\OneDrive\Bureau\Mabrouk Hannachi\data-quality-pipeline\sql\quality_kpis.sql"

def run_kpis():
    print("--- CALCUL DES KPIs (RETAIL) ---")
    
    # 1. Read SQL File
    if not os.path.exists(SQL_FILE):
        print(f"Error: SQL file not found at {SQL_FILE}")
        return
        
    with open(SQL_FILE, 'r') as f:
        sql_content = f.read()
        
    # 2. Split into statements (semicolon)
    statements = sql_content.split(';')
    
    # 3. Connect and Execute
    try:
        conn = pymysql.connect(host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASSWORD, database=DB_NAME)
        cursor = conn.cursor()
        
        print(f"Connected to {DB_NAME}. Executing statements...")
        
        count = 0
        for stmt in statements:
            if stmt.strip():
                try:
                    cursor.execute(stmt)
                    count += 1
                except Exception as e:
                    print(f"Error executing statement: {stmt[:50]}...\n{e}")
        
        conn.commit()
        print(f"Success: Executed {count} SQL statements.")
        print("Metrics tables (quality_metrics, quality_scores_history) should now be populated.")
        
        conn.close()
        
    except Exception as e:
        print(f"Database connection error: {e}")

if __name__ == "__main__":
    run_kpis()
