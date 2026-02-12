import pymysql

DB_HOST = "localhost"
DB_PORT = 3307
DB_USER = "dq_user"
DB_PASSWORD = "dq_password"
DB_NAME = "data_quality"

def check_data():
    conn = pymysql.connect(host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASSWORD, database=DB_NAME)
    cursor = conn.cursor()

    print("--- CHECKING QUALITY METRICS ---")
    cursor.execute("SELECT DISTINCT pillar FROM quality_metrics")
    pillars = cursor.fetchall()
    print(f"Pillars found: {pillars}")
    
    print("\n--- CHECKING HISTORY ---")
    cursor.execute("SELECT * FROM quality_scores_history LIMIT 1")
    row = cursor.fetchone()
    print(f"History Row: {row}")
    
    # Check column names
    cursor.execute("DESCRIBE quality_scores_history")
    cols = cursor.fetchall()
    print(f"History Columns: {[c[0] for c in cols]}")

    conn.close()

if __name__ == "__main__":
    check_data()
