import logging

from airflow.decorators import dag, task


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
}


@dag(
    dag_id='test_1c_dag',
    default_args=default_args,
    schedule_interval=None,
    max_active_runs=1,
)
def test_1c_dag():
    @task
    def test_1c_task():
        import os
        from dotenv import load_dotenv

        load_dotenv()
        server = os.getenv('DB_SERVER')
        username = os.getenv('DB_USER')
        password = os.getenv('DB_PASSWORD')
        database = os.getenv('DB_NAME')

        import pyodbc

        # Строка подключения
        conn_str = (
            r'DRIVER={SQL Server};'
            r'SERVER=' + server + ';'
            r'DATABASE=' + database + ';'
            r'UID=' + username + ';'
            r'PWD=' + password + ';'
        )

        # Создание подключения
        conn = pyodbc.connect(conn_str)

        # Создание курсора
        cursor = conn.cursor()

        # Выполнение запроса
        cursor.execute("SELECT 1")

        # Получение результатаca
        result = cursor.fetchone()

        # Вывод результата
        print(result)

        # Закрытие курсора и подключения
        cursor.close()
        conn.close()

    test_1c_task()


test_1c_dag()
