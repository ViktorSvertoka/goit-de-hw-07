from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.sensors.python import PythonSensor
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import random
import time
import mysql.connector

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

dag = DAG(
    "olympic_medals_dag",
    default_args=default_args,
    description="DAG for Olympic medal counting",
    schedule_interval="@once",
    start_date=days_ago(1),
    catchup=False,
)


# Підключення до MySQL
def get_mysql_connection():
    return mysql.connector.connect(
        host="localhost", user="root", password="password", database="olympic_dataset"
    )


# 1. Створення таблиці
create_table = MySqlOperator(
    task_id="create_table",
    mysql_conn_id="mysql_default",
    sql="""
    CREATE TABLE IF NOT EXISTS medal_count (
        id INT AUTO_INCREMENT PRIMARY KEY,
        medal_type VARCHAR(10),
        count INT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """,
    dag=dag,
)


# 2. Генерація випадкового значення
def choose_medal_type():
    return random.choice(["Bronze", "Silver", "Gold"])


choose_medal = PythonOperator(
    task_id="choose_medal", python_callable=choose_medal_type, dag=dag
)


# 3. Розгалуження
def branch_task(**kwargs):
    medal = kwargs["ti"].xcom_pull(task_ids="choose_medal")
    if medal == "Bronze":
        return "count_bronze"
    elif medal == "Silver":
        return "count_silver"
    else:
        return "count_gold"


branching = BranchPythonOperator(
    task_id="branching", python_callable=branch_task, provide_context=True, dag=dag
)


# 4. Завдання для підрахунку медалей
def count_medals(medal_type):
    conn = get_mysql_connection()
    cursor = conn.cursor()
    query = f"SELECT COUNT(*) FROM athlete_event_results WHERE medal = '{medal_type}'"
    cursor.execute(query)
    count = cursor.fetchone()[0]

    insert_query = """
    INSERT INTO medal_count (medal_type, count) 
    VALUES (%s, %s)
    """
    cursor.execute(insert_query, (medal_type, count))
    conn.commit()
    cursor.close()
    conn.close()


count_bronze = PythonOperator(
    task_id="count_bronze", python_callable=lambda: count_medals("Bronze"), dag=dag
)

count_silver = PythonOperator(
    task_id="count_silver", python_callable=lambda: count_medals("Silver"), dag=dag
)

count_gold = PythonOperator(
    task_id="count_gold", python_callable=lambda: count_medals("Gold"), dag=dag
)


# 5. Затримка виконання
def sleep_task():
    time.sleep(35)


delay = PythonOperator(task_id="delay", python_callable=sleep_task, dag=dag)


# 6. Сенсор для перевірки часу запису
def is_recent_record():
    conn = get_mysql_connection()
    cursor = conn.cursor()
    query = "SELECT created_at FROM medal_count ORDER BY created_at DESC LIMIT 1"
    cursor.execute(query)
    latest_timestamp = cursor.fetchone()
    if not latest_timestamp:
        return False
    latest_timestamp = latest_timestamp[0]
    cursor.close()
    conn.close()
    return (datetime.now() - latest_timestamp).seconds <= 30


check_recent = PythonSensor(
    task_id="check_recent",
    python_callable=is_recent_record,
    mode="poke",
    timeout=60,
    poke_interval=5,
    dag=dag,
)

# Зв'язок між задачами
create_table >> choose_medal >> branching
branching >> [count_bronze, count_silver, count_gold] >> delay >> check_recent
