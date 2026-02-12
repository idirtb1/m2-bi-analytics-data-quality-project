"""
DAG Airflow - Pipeline Data Quality
Orchestration automatique du pipeline de qualité des données
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# Configuration par défaut du DAG
default_args = {
    'owner': 'data_quality_team',
    'depends_on_past': False,
    'email': ['admin@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Définition du DAG
dag = DAG(
    'data_quality_pipeline',
    default_args=default_args,
    description='Pipeline de Data Quality pour Retail Store Sales',
    schedule_interval='0 2 * * *',  # Execution quotidienne a 2h du matin
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['data-quality', 'retail', 'etl'],
)

# ========================================
# TASK GROUP 1 : Import et Nettoyage
# ========================================

import_data = BashOperator(
    task_id='import_raw_data',
    bash_command='python /opt/airflow/scripts/import_data.py',
    dag=dag,
)

clean_data = BashOperator(
    task_id='clean_data_6_pillars',
    bash_command='python /opt/airflow/scripts/cleaning_pipeline.py',
    dag=dag,
)

# ========================================
# TASK GROUP 2 : Validation
# ========================================

run_expectations = BashOperator(
    task_id='run_great_expectations',
    bash_command='python /opt/airflow/scripts/great_expectations_validator.py',
    dag=dag,
)

# ========================================
# TASK GROUP 3 : KPI et Métriques
# ========================================

def calculate_quality_kpis(**kwargs):
    """Calcule les KPI de qualite et les stocke dans MariaDB"""
    import pymysql
    import pandas as pd
    
    conn = pymysql.connect(
        host='dq_mariadb', port=3306,
        user='dq_user', password='dq_password',
        database='data_quality'
    )
    df = pd.read_sql('SELECT * FROM retail_cleaned', conn)
    
    kpis = {
        'total_records': len(df),
        'null_count': int(df.isnull().sum().sum()),
        'duplicate_count': int(df.duplicated().sum()),
        'completeness': round((1 - df.isnull().sum().sum() / (len(df) * len(df.columns))) * 100, 2),
        'uniqueness': round((df['Transaction_ID'].nunique() / len(df)) * 100, 2),
    }
    conn.close()
    
    print(f"KPIs calcules: {kpis}")
    return kpis

calculate_kpis = PythonOperator(
    task_id='calculate_quality_kpis',
    python_callable=calculate_quality_kpis,
    dag=dag,
)

# ========================================
# TASK GROUP 4 : Rapports et Alertes
# ========================================

generate_dashboard = BashOperator(
    task_id='generate_dashboard',
    bash_command='python /opt/airflow/scripts/dashboard_generator.py',
    dag=dag,
)

def check_quality_alerts(**kwargs):
    """Vérifie si des alertes doivent être envoyées"""
    ti = kwargs['ti']
    kpis = ti.xcom_pull(task_ids='calculate_quality_kpis')
    
    alerts = []
    
    if kpis and kpis.get('completeness', 100) < 95:
        alerts.append(f"⚠️ Complétude sous 95%: {kpis['completeness']:.2f}%")
    
    if kpis and kpis.get('duplicate_count', 0) > 0:
        alerts.append(f"⚠️ {kpis['duplicate_count']} doublons détectés")
    
    if alerts:
        print("ALERTES QUALITÉ:")
        for alert in alerts:
            print(alert)
        # Ici on pourrait envoyer un email ou une notification Slack
    else:
        print("✅ Aucune alerte - Qualité OK")
    
    return alerts

send_alerts = PythonOperator(
    task_id='check_and_send_alerts',
    python_callable=check_quality_alerts,
    dag=dag,
)

# ========================================
# TASK GROUP 5 : Archivage
# ========================================

archive_results = BashOperator(
    task_id='archive_daily_results',
    bash_command='''
        DATE=$(date +%Y%m%d)
        mkdir -p /opt/airflow/archives/$DATE
        cp /opt/airflow/reports/*.html /opt/airflow/archives/$DATE/
        cp /opt/airflow/reports/*.json /opt/airflow/archives/$DATE/
        echo "Résultats archivés pour $DATE"
    ''',
    dag=dag,
)

# ========================================
# DÉPENDANCES (Ordre d'exécution)
# ========================================

# Flux principal du pipeline:
# 
#  import_data
#       ↓
#  clean_data
#       ↓
#  run_expectations
#       ↓
#  calculate_kpis
#       ↓
# ┌─────┴─────┐
# ↓           ↓
# generate    send
# dashboard   alerts
# ↓           ↓
# └─────┬─────┘
#       ↓
#  archive_results

import_data >> clean_data >> run_expectations >> calculate_kpis
calculate_kpis >> [generate_dashboard, send_alerts]
[generate_dashboard, send_alerts] >> archive_results
