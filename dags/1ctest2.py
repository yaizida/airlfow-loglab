import os
import io
import csv
import logging
import pprint
import tracemalloc
import time

from pymssql import connect, Connection
from dotenv import load_dotenv
import pandas as pd
from memory_profiler import profile


@profile
def create_connection() -> csv.writer:
    # read data from .env file
    load_dotenv()
    db_server = os.getenv('DB_SERVER')
    db_user = os.getenv('DB_USER')
    db_password = os.getenv('DB_PASSWORD')
    db_name = os.getenv('DB_NAME')

    # connect to the SQL Server
    try:
        conn = connect(server=db_server, user=db_user,
                       password=db_password, database=db_name)
    except Exception as e:
        logging.error(f'Error connecting to the SQL Server database: {e}')
        return None

    # Use cursor to execute SQL query
    cursor = conn.cursor()

    sql_query = """
        SELECT *
        FROM dbo.Config
    """

    cursor.execute(sql_query)

    # Fetch all rows from the result
    rows = cursor.fetchall()

    columns = [x[0] for x in cursor.description]

    s_buf = io.StringIO()
    writer = csv.writer(s_buf)

    writer.writerow(columns)

    for row in rows:
        writer.writerow([str(x) if str(x)[:2] != "b'" else f'"{str(x)}"' for x in row])

    s_buf.seek(0)

    cursor.close()
    conn.close()

    return writer


if __name__ == '__main__':
    tracemalloc.start()
    start_time = time.time()
    new_writer = create_connection()
    snapshot = tracemalloc.take_snapshot()
    top_stats = snapshot.statistics('traceback')

    print("Аллокация памяти:")

    for stat in top_stats[:5]:
        print(stat)

    # Остановка отслеживания памяти
    tracemalloc.stop()

    end_time = time.time()
    print(f"Время выполнения: {end_time - start_time:.4f} секунд")
    pprint.pprint(new_writer)
    print("CSV file created successfully.")  # Indicate successful export
