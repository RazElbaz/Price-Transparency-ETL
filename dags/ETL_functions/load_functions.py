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
        INSERT INTO stores (StoreId, PriceUpdateDate, ItemCode, ItemType, ItemName, ItemPrice, SupermarketChain)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """

    try:
        for item in transformed_data:
            cur.execute(sql_template, (
                item['StoreId'],
                item['PriceUpdateDate'],
                item['ItemCode'],
                item['ItemType'],
                item['ItemName'],
                item['ItemPrice'],
                item['SupermarketChain']
            ))
        conn.commit()
        print("Data inserted successfully into PostgreSQL")
    except Exception as e:
        conn.rollback()
        print(f"Error inserting data into PostgreSQL: {e}")
    # finally:
    #     cur.close()
    #     conn.close()

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
            ItemPrice FLOAT,
            SupermarketChain VARCHAR(50)
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
    transformed_data_victory = kwargs['task_instance'].xcom_pull(task_ids='transform_data_victory', key='')

    conn, cur = connect_to_postgres()

    try:
        insert_data_to_postgres(conn, cur, transformed_data)
        insert_data_to_postgres(conn, cur, transformed_data_victory)
        get_common_products_and_cheapest_basket()
        # calculate_total_branch_prices_for_same_items()
    finally:
        cur.close()
        conn.close()



def get_common_products_and_cheapest_basket():
    conn, cur = connect_to_postgres()

    try:
        # Step 1: Get the list of common items across more than 50 branches
        common_items_query = """
            WITH common_items AS (
                -- Step 1: Identify the list of items common across most branches
                SELECT ItemCode, ItemName
                FROM (
                    SELECT ItemCode, ItemName, COUNT(DISTINCT StoreId) AS num_branches
                    FROM stores
                    GROUP BY ItemCode, ItemName
                    ORDER BY COUNT(DISTINCT StoreId) DESC
                    LIMIT 30 -- Adjust this limit to get the top items by number of branches
                ) AS top_items
            )
            -- Query to display the list of common items
            SELECT ItemCode, ItemName
            FROM common_items
        """
        cur.execute(common_items_query)
        common_items = cur.fetchall()

        print("Common Products found across branches:")
        for item in common_items:
            print(item)

        # Step 2: Find the total price of these common items in each branch
        total_branch_prices_query = """
            WITH common_items AS (
                -- Step 1: Identify the list of items common across most branches
                SELECT ItemCode, ItemName
                FROM (
                    SELECT ItemCode, ItemName, COUNT(DISTINCT StoreId) AS num_branches
                    FROM stores
                    GROUP BY ItemCode, ItemName
                    ORDER BY COUNT(DISTINCT StoreId) DESC
                    LIMIT 30 -- Adjust this limit to get the top items by number of branches
                ) AS top_items
            ), 
            branches_with_common_items AS (
                -- Step 2: Find branches that have all the common items
                SELECT s.StoreId, s.SupermarketChain, SUM(s.ItemPrice) AS TotalPrice
                FROM stores s
                JOIN common_items ci ON s.ItemCode = ci.ItemCode AND s.ItemName = ci.ItemName
                GROUP BY s.StoreId, s.SupermarketChain
                HAVING COUNT(DISTINCT s.ItemCode || s.ItemName) = (SELECT COUNT(*) FROM common_items)
            )
            -- Step 3: Find the branch with the cheapest sum of item prices
            SELECT StoreId, SupermarketChain, TotalPrice
            FROM branches_with_common_items
            ORDER BY TotalPrice ASC
            LIMIT 1
        """
        cur.execute(total_branch_prices_query)
        cheapest_basket = cur.fetchone()

        print("\nBranch with the Cheapest Basket of Common Items:")
        print(f"StoreId: {cheapest_basket[0]}, SupermarketChain: {cheapest_basket[1]}, TotalPrice: {cheapest_basket[2]}")

        return common_items, cheapest_basket

    except Exception as e:
        print(f"Error querying data from PostgreSQL: {e}")
    finally:
        cur.close()
        conn.close()

# for test
# WITH common_items AS (
#     -- Step 1: Identify the list of top 15 items common across most branches
#     SELECT ItemCode, ItemName
#     FROM (
#         SELECT ItemCode, ItemName, COUNT(DISTINCT StoreId) AS num_branches
#         FROM stores
#         GROUP BY ItemCode, ItemName
#         ORDER BY COUNT(DISTINCT StoreId) DESC
#         LIMIT 15 -- Adjust this limit to get the top 15 items
#     ) AS top_items
# ),
# branches_with_common_items AS (
#     -- Step 2: Find branches that have all the top 15 common items
#     SELECT s.StoreId, ARRAY_AGG(s.ItemCode || ' - ' || s.ItemName) AS common_items_list, SUM(s.ItemPrice) AS TotalPrice
#     FROM stores s
#     JOIN common_items ci ON s.ItemCode = ci.ItemCode AND s.ItemName = ci.ItemName
#     GROUP BY s.StoreId
#     HAVING COUNT(DISTINCT s.ItemCode || s.ItemName) = (SELECT COUNT(*) FROM common_items)
# )
# -- Step 3: Fetch and display the results
# SELECT StoreId, TotalPrice, common_items_list
# FROM branches_with_common_items
# ORDER BY TotalPrice ASC;
# """
#             WITH common_items AS (
#                 -- Step 1: Identify the list of items common across most branches
#                 SELECT ItemCode, ItemName
#                 FROM (
#                     SELECT ItemCode, ItemName, COUNT(DISTINCT StoreId) AS num_branches
#                     FROM stores
#                     GROUP BY ItemCode, ItemName
#                     ORDER BY COUNT(DISTINCT StoreId) DESC
#                     LIMIT 30 -- Adjust this limit to get the top items by number of branches
#                 ) AS top_items
#             ), 
#             branches_with_common_items AS (
#                 -- Step 2: Find branches that have all the common items
#                 SELECT s.StoreId, ARRAY_AGG(s.ItemCode || ' - ' || s.ItemName) AS common_items_list, SUM(s.ItemPrice) AS TotalPrice
#                 FROM stores s
#                 JOIN common_items ci ON s.ItemCode = ci.ItemCode AND s.ItemName = ci.ItemName
#                 GROUP BY s.StoreId
#                 HAVING COUNT(DISTINCT s.ItemCode || s.ItemName) = (SELECT COUNT(*) FROM common_items)
#             )
#             -- Step 3: Find the branch with the cheapest sum of item prices
#             SELECT StoreId, TotalPrice, common_items_list
#             FROM branches_with_common_items
#             ORDER BY TotalPrice ASC
#             LIMIT 1

#         """

#
