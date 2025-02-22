import os
import io
import csv
import logging

from pymssql import connect, Connection
from dotenv import load_dotenv
import pandas as pd
from memory_profiler import profile


@profile
def create_connection() -> None:
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

    # Create or open the CSV file
    with open(r"C:\Users\nikita.b\Desktop\Dev\LogLab\airlfow-loglab\dags\MyTable.csv", 'w', newline='') as csvfile:
        csvfile.write(';'.join(columns) + '\n')
        for row in rows:
            csvfile.write(';'.join([str(x) if str(x)[:2] != "b'" else f'"{str(x)}"' for x in row]) + '\n')

    cursor.close()
    conn.close()

    return


if __name__ == '__main__':
    create_connection()
    print("CSV file created successfully.")  # Indicate successful export
