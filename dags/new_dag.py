from datetime import datetime
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

        from pymssql import connect
        from dotenv import load_dotenv

        load_dotenv()
        db_server = os.getenv('DB_SERVER')
        db_user = os.getenv('DB_USER')
        db_password = os.getenv('DB_PASSWORD')
        db_name = os.getenv('DB_NAME')

        # connect to the SQL Server
        try:
            conn = connect(server=db_server, user=db_user,
                           password=db_password, database=db_name,
                           )
        except Exception as e:
            logging.error(f'Error connecting to the SQL Server database: {e}')

        cursor = conn.cursor()

        cursor.execute('SELECT 1')

        result = cursor.fetchone()

        for row in result:
            print(row)

        cursor.close()
        conn.close()

    test_1c_task()


test_1c_dag()
