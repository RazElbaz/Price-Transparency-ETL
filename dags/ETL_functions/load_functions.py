import psycopg2
import os

def connect_to_postgres():
    """
    Establishes a connection to PostgreSQL and returns the connection and cursor objects.
    """
    conn = psycopg2.connect(
        host='postgres',
        port='5432',
        dbname='airflow',
        user='airflow',
        password='airflow'
    )
    return conn, conn.cursor()

def insert_data_to_postgres(conn, cur, transformed_data):
    """
    Inserts transformed data into PostgreSQL.

    Args:
    - conn: PostgreSQL connection object.
    - cur: PostgreSQL cursor object.
    - transformed_data: List of dictionaries containing data to insert.
    """
    sql_template = """
        INSERT INTO stores (StoreId, PriceUpdateDate, ItemCode, ItemType, ItemName, ItemPrice)
        VALUES (%s, %s, %s, %s, %s, %s)
    """

    try:
        for item in transformed_data:
            cur.execute(sql_template, (
                item['StoreId'],
                item['PriceUpdateDate'],
                item['ItemCode'],
                item['ItemType'],
                item['ItemName'],
                item['ItemPrice']
            ))
        conn.commit()
        print("Data inserted successfully into PostgreSQL")
    except Exception as e:
        conn.rollback()
        print(f"Error inserting data into PostgreSQL: {e}")
    finally:
        cur.close()
        conn.close()

def create_postgres_table():
    """
    Creates 'stores' table in PostgreSQL if it doesn't exist.
    """
    conn, cur = connect_to_postgres()

    create_table_sql = """
        CREATE TABLE IF NOT EXISTS stores (
            StoreId VARCHAR(50),
            PriceUpdateDate TIMESTAMP,
            ItemCode VARCHAR(50),
            ItemType INT,
            ItemName VARCHAR(255),
            ItemPrice FLOAT
        )
    """

    try:
        cur.execute(create_table_sql)
        conn.commit()
        print("Table 'stores' created successfully in PostgreSQL")
    except Exception as e:
        conn.rollback()
        print(f"Error creating table in PostgreSQL: {e}")
    finally:
        cur.close()
        conn.close()

def extract_postgres_data_to_file(output_filepath):
    """
    Extracts data from 'stores' table in PostgreSQL and writes it to a CSV file.

    Args:
    - output_filepath: Filepath where the extracted data will be written.
    """
    conn, cur = connect_to_postgres()

    query = "SELECT * FROM stores"

    try:
        cur.execute(query)
        rows = cur.fetchall()

        with open(output_filepath, 'w') as f:
            for row in rows:
                f.write(','.join(map(str, row)) + '\n')

        print(f"Data extracted successfully to {output_filepath}")
    except Exception as e:
        print(f"Error extracting data from PostgreSQL: {e}")
    finally:
        cur.close()
        conn.close()


def load_to_postgres(**kwargs):
    # transformed_data = kwargs['task_instance'].xcom_pull(task_ids='transform_data', key='transformed_data')
    transformed_data = kwargs['task_instance'].xcom_pull(task_ids='transform_data', key='transformed_data')

    conn, cur = connect_to_postgres()

    try:
        insert_data_to_postgres(conn, cur, transformed_data)
    finally:
        cur.close()
        conn.close()