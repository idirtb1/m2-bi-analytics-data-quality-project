"""
Moniteur d'alertes Data Quality
Verifie quotidiennement si le score DQ est au dessus du seuil (90%)
Usage: python scripts/dq_alert_monitor.py
"""
import pymysql
import json
import os
from datetime import datetime

# Configuration
THRESHOLD = 90.0
DB_CONFIG = {
    "host": "localhost",
    "port": 3307,
    "user": "dq_user",
    "password": "dq_password",
    "database": "data_quality"
}

def check_quality_score():
    """Verifie le score de qualite et declenche une alerte si necessaire"""
    conn = pymysql.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    # Get latest score (average of all pillars)
    cur.execute("""
        SELECT 
            report_date,
            completeness,
            accuracy,
            uniqueness,
            validity,
            consistency,
            timeliness,
            (completeness + accuracy + uniqueness + validity + consistency + timeliness) / 6.0 as global_score
        FROM quality_scores_history 
        ORDER BY report_date DESC 
        LIMIT 1
    """)
    result = cur.fetchone()
    conn.close()
    
    if not result:
        print("[WARN] Aucun score trouve dans quality_scores_history")
        return None
    
    report_date = result[0]
    pillars = {
        "Completude": float(result[1]),
        "Exactitude": float(result[2]),
        "Unicite": float(result[3]),
        "Validite": float(result[4]),
        "Coherence": float(result[5]),
        "Actualite": float(result[6]),
    }
    global_score = float(result[7])
    
    now = datetime.now().strftime('%Y-%m-%d %H:%M')
    print(f"\n{'='*50}")
    print(f"  MONITEUR QUALITE DES DONNEES")
    print(f"  {now}")
    print(f"{'='*50}")
    print(f"\n  Score Global : {global_score:.2f}%")
    print(f"  Seuil Alerte : {THRESHOLD}%")
    print(f"  Date Rapport : {report_date}")
    print(f"\n  Detail par pilier :")
    for name, val in pillars.items():
        icon = "OK" if val >= THRESHOLD else "!!"
        print(f"    [{icon}] {name}: {val:.1f}%")
    
    alert = {
        "timestamp": datetime.now().isoformat(),
        "global_score": round(global_score, 2),
        "threshold": THRESHOLD,
        "status": "OK" if global_score >= THRESHOLD else "ALERTE",
        "report_date": str(report_date),
        "pillars": pillars
    }
    
    if global_score < THRESHOLD:
        print(f"\n  *** ALERTE: Score ({global_score:.2f}%) < Seuil ({THRESHOLD}%) ***")
        # Identify weak pillars
        weak = [n for n, v in pillars.items() if v < THRESHOLD]
        if weak:
            print(f"  Piliers faibles: {', '.join(weak)}")
        print(f"  Action: Verifier les donnees et relancer le nettoyage.")
    else:
        print(f"\n  Status: OK - Score au dessus du seuil")
    
    # Save alert log
    log_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "logs")
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, "dq_alerts.json")
    
    alerts = []
    if os.path.exists(log_file):
        with open(log_file, "r", encoding="utf-8") as f:
            try:
                alerts = json.load(f)
            except:
                alerts = []
    
    alerts.append(alert)
    
    with open(log_file, "w", encoding="utf-8") as f:
        json.dump(alerts, f, indent=2, ensure_ascii=False)
    
    print(f"\n  Log sauvegarde: {log_file}")
    print(f"{'='*50}\n")
    return alert

if __name__ == "__main__":
    check_quality_score()
