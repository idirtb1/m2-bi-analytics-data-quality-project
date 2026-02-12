import pandas as pd
import pymysql
import numpy as np

DB_HOST = "localhost"
DB_PORT = 3307
DB_USER = "dq_user"
DB_PASSWORD = "dq_password"
DB_NAME = "data_quality"

CSV_PATH = r"c:\Users\idirt\OneDrive\Bureau\Mabrouk Hannachi\data-quality-pipeline\data\raw\Retail_Store_Sales.csv"

def import_data():
    print("=" * 60)
    print("IMPORT DES DONNÉES RETAIL")
    print("=" * 60)

    print("\n[1/3] Chargement du CSV...")
    df = pd.read_csv(CSV_PATH)
    print(f"  {len(df)} lignes, {len(df.columns)} colonnes chargées")
    print(f"  Colonnes : {list(df.columns)}")

    df = df.replace({np.nan: None})

    print("\n[2/3] Connexion à MariaDB...")
    try:
        conn = pymysql.connect(
            host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASSWORD, database=DB_NAME
        )
        cursor = conn.cursor()
        print("  Connexion réussie")
    except Exception as e:
        print(f"  ERREUR DE CONNEXION : {e}")
        return

    print("\n[3/3] Insertion dans retail_raw...")
    try:
        cursor.execute("TRUNCATE TABLE retail_raw")

        cols = [
            "Transaction_ID", "Customer_ID", "Customer_Name", "Product_Category",
            "Product_Name", "Unit_Price", "Quantity", "Total_Amount",
            "Payment_Method", "City", "Transaction_Date"
        ]
        placeholders = ", ".join(["%s"] * len(cols))
        sql = f"INSERT INTO retail_raw ({', '.join(cols)}) VALUES ({placeholders})"

        batch_size = 1000
        batch = []
        count = 0

        for _, row in df.iterrows():
            values = tuple(row[c] for c in cols)
            batch.append(values)

            if len(batch) >= batch_size:
                cursor.executemany(sql, batch)
                conn.commit()
                count += len(batch)
                print(f"  {count} / {len(df)} lignes insérées...")
                batch = []

        if batch:
            cursor.executemany(sql, batch)
            conn.commit()
            count += len(batch)
            print(f"  {count} / {len(df)} lignes insérées...")

        print(f"  Import terminé : {count} lignes.")

    except Exception as e:
        print(f"  ERREUR D'INSERTION : {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    import_data()
