from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

# Définition des arguments par défaut du DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 4, 4),
}

# Création du DAG
dag = DAG(
    'execute_pyspark_script',
    default_args=default_args,
    description='Exécute le script PySpark',
    schedule_interval=timedelta(days=1),  # Exécute tous les jours
)

# Tâche pour exécuter le script PySpark avec spark-submit
execute_pyspark_task = BashOperator(
    task_id='execute_pyspark_script',
    bash_command=r'spark-submit --master local[*] C:\Users\MouhamethSECK\Desktop\transformadata.PY',
    dag=dag,
)

# Définissez l'ordre des tâches dans le DAG
execute_pyspark_task
