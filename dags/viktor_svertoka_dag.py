from airflow import DAG
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.utils.state import State
import random
import time
from airflow.utils.dates import days_ago


# Функція для примусового встановлення статусу DAG на SUCCESS
# Це корисно для тестування та форсування завершення DAG, навіть якщо є помилки.
def force_success_status(ti, **kwargs):
    dag_run = kwargs["dag_run"]
    dag_run.set_state(State.SUCCESS)


# Функція, яка випадково вибирає тип медалі
# Використовується для симуляції випадкового процесу в даних.
def random_medal_choice():
    return random.choice(["Gold", "Silver", "Bronze"])


# Функція для імітації затримки обробки
# Додає затримку виконання для створення більш реалістичного сценарію обробки даних.
def delay_execution():
    time.sleep(35)


# Базові параметри DAG, зокрема вказуємо власника та стартову дату.
default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
}

# Назва з'єднання для MySQL (має бути налаштоване в Airflow Connections).
mysql_connection_id = "mysql_connection_viktor"

# Опис самого DAG
with DAG(
    "viktor_svertoka_dag2",
    default_args=default_args,
    schedule_interval=None,  # Немає автоматичного розкладу виконання
    catchup=False,  # Виконуються тільки актуальні запуски DAG
    tags=["viktor_medal_counting2"],  # Тег для зручного пошуку в інтерфейсі Airflow
) as dag:

    # Завдання 1: Створення таблиці для зберігання даних про медалі
    # Таблиця використовується для зберігання підрахунків медалей у базі даних.
    create_table_task = MySqlOperator(
        task_id="create_medal_table",
        mysql_conn_id=mysql_connection_id,
        sql="""
        CREATE TABLE IF NOT EXISTS neo_data.viktor_svertoka_medal_counts (
            id INT AUTO_INCREMENT PRIMARY KEY,
            medal_type VARCHAR(10),
            medal_count INT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """,
    )

    # Завдання 2: Випадковий вибір типу медалі (золото, срібло, бронза)
    # Це визначає, яке завдання буде виконане далі в процесі DAG.
    select_medal_task = PythonOperator(
        task_id="select_medal",
        python_callable=random_medal_choice,
    )

    # Завдання 3: Розгалуження на основі вибраної медалі
    # В залежності від обраного типу медалі виконується відповідне завдання.
    def branching_logic(**kwargs):
        selected_medal = kwargs["ti"].xcom_pull(task_ids="select_medal")
        if selected_medal == "Gold":
            return "count_gold_medals"
        elif selected_medal == "Silver":
            return "count_silver_medals"
        else:
            return "count_bronze_medals"

    branching_task = BranchPythonOperator(
        task_id="branch_based_on_medal",
        python_callable=branching_logic,
        provide_context=True,
    )

    # Завдання 4: Підрахунок бронзових медалей
    # Виконує SQL-запит для підрахунку всіх бронзових медалей у таблиці.
    count_bronze_task = MySqlOperator(
        task_id="count_bronze_medals",
        mysql_conn_id=mysql_connection_id,
        sql="""
           INSERT INTO neo_data.viktor_svertoka_medal_counts (medal_type, medal_count)
           SELECT 'Bronze', COUNT(*)
           FROM olympic_dataset.athlete_event_results
           WHERE medal = 'Bronze';
           """,
    )

    # Завдання 5: Підрахунок срібних медалей
    # Виконує SQL-запит для підрахунку всіх срібних медалей у таблиці.
    count_silver_task = MySqlOperator(
        task_id="count_silver_medals",
        mysql_conn_id=mysql_connection_id,
        sql="""
           INSERT INTO neo_data.viktor_svertoka_medal_counts (medal_type, medal_count)
           SELECT 'Silver', COUNT(*)
           FROM olympic_dataset.athlete_event_results
           WHERE medal = 'Silver';
           """,
    )

    # Завдання 6: Підрахунок золотих медалей
    # Виконує SQL-запит для підрахунку всіх золотих медалей у таблиці.
    count_gold_task = MySqlOperator(
        task_id="count_gold_medals",
        mysql_conn_id=mysql_connection_id,
        sql="""
           INSERT INTO neo_data.viktor_svertoka_medal_counts (medal_type, medal_count)
           SELECT 'Gold', COUNT(*)
           FROM olympic_dataset.athlete_event_results
           WHERE medal = 'Gold';
           """,
    )

    # Завдання 7: Затримка обробки (імітація складного процесу)
    # Використовується для моделювання реальної затримки в обробці даних.
    delay_task = PythonOperator(
        task_id="delay_task",
        python_callable=delay_execution,
        trigger_rule=TriggerRule.ONE_SUCCESS,  # Виконується, якщо хоча б одне попереднє завдання успішне
    )

    # Завдання 8: Перевірка наявності записів у таблиці
    # Сенсор перевіряє, чи з'явилися нові записи за останні 30 секунд.
    check_last_record_task = SqlSensor(
        task_id="verify_recent_record",
        conn_id=mysql_connection_id,
        sql="""
            WITH count_in_medals AS (
                SELECT COUNT(*) as nrows
                FROM neo_data.viktor_svertoka_medal_counts
                WHERE created_at >= NOW() - INTERVAL 30 SECOND
            )
            SELECT nrows > 0 FROM count_in_medals;
        """,
        mode="poke",  # Перевірка умови періодично
        poke_interval=10,  # Інтервал перевірки (10 секунд)
        timeout=30,  # Тайм-аут перевірки (30 секунд)
    )

    # Визначення послідовності виконання завдань у DAG
    create_table_task >> select_medal_task >> branching_task
    (
        branching_task
        >> [count_bronze_task, count_silver_task, count_gold_task]
        >> delay_task
    )
    delay_task >> check_last_record_task
