# Price Transparency ETL

## Overview

The **Price Transparency ETL** project aims to enhance price transparency in the retail sector by developing an efficient ETL (Extract, Transform, Load) pipeline. This project collects, processes, and analyzes price data from various supermarket companies, making it easier for consumers to compare prices and make informed purchasing decisions. By promoting competition and price transparency, this project contributes to the fight against the cost of living.

## Components

### ETL Workflow

The ETL pipeline consists of the following main tasks:

1. **Extract**: Downloads price data from the Shufersal and Victory websites.
2. **Transform**: Parses XML files, transforms and standardizes the data.
3. **Load**: Inserts the transformed data into a PostgreSQL database.
4. **PostgreSQL Operations**: Creates necessary tables in PostgreSQL and handles data loading.
5. **Analytics**: Identifies common products across branches and determines the cheapest basket.

### Directory Structure

- **dags/**: Contains the Airflow DAG definition file (`etl_dag.py`).
- **ETL_functions/**:
  - **extract_functions_shufersal.py**: Contains functions for extracting data from the Shufersal website.
  - **extract_functions_victory.py**: Contains functions for extracting data from the Victory website.
  - **load_functions.py**: Includes functions for PostgreSQL operations, such as creating tables and loading data.
  - **transform_functions_shufersal.py**: Contains functions for transforming XML data extracted from Shufersal.
  - **transform_functions_victory.py**: Contains functions for transforming XML data extracted from Victory.

### Docker Configuration

The project is containerized using Docker Compose, with the following services:

- **postgres**: PostgreSQL database (version 9.6) for storing transformed data.
- **webserver**: Airflow webserver container with local executor for managing and scheduling the ETL workflows.

## Getting Started

## DAG Details

The DAG `branches_extraction` performs the following tasks:
1. **Create PostgreSQL Table**: Creates a table in PostgreSQL to store the pricing data.
2. **Extract Data**:
   - Extracts pricing data from the Shufersal website using `extract_data_shufersal`.
   - Extracts pricing data from the Victory website using `extract_data_victory`.
3. **Transform Data**:
   - Transforms the extracted XML data from Shufersal into a format suitable for loading into PostgreSQL with `transform_data_shufersal`.
   - Transforms the extracted XML data from Victory into a format suitable for loading into PostgreSQL with `transform_data_victory`.
4. **Load Data**: Loads the transformed data into the PostgreSQL database using `load_to_postgres`.
5. **Get Common Products and Cheapest Basket**: Retrieves common products across branches and identifies the branch with the cheapest basket using `get_common_products_and_cheapest_basket`.
6. **Clear XML Files**: Clears the downloaded XML files to save space using `clear_xml_files_directory`.

### Prerequisites

- Docker and Docker Compose installed on your machine.

### Installation and Setup

1. **Clone the repository**:
   ```bash
   git clone https://github.com/RazElbaz/price-transparency-etl.git
   cd price-transparency-etl
   ```

2. **Build and run containers**:
   ```bash
   docker-compose up --build
   ```
   This command builds and starts the PostgreSQL and Airflow services defined in `docker-compose.yml`.

3. **Access Airflow Web UI**:
   Open a web browser and go to `http://localhost:8080` to access the Airflow web interface.

4. **Configure Airflow DAG**:
   - Navigate to the Airflow UI (`localhost:8080`).
   - Enable the `branches_extraction` DAG.
   - Trigger the DAG manually or wait for the scheduled interval (`0 */2 * * *` by default) to start the ETL process.

5. **Monitor and Manage DAG**:
   Use the Airflow UI to monitor DAG runs, view task logs, and manage workflow execution.

### Stopping the Services

To stop the running Docker Compose services, use the following command:

```bash
docker-compose down
```

This command stops and removes all the containers defined in the `docker-compose.yml` file.

### Viewing Airflow Task Logs

To view the Airflow task logs generated during DAG execution, follow these steps:

1. **Access Logs Inside Docker Container**:
   - Use `docker exec` to access the Airflow container:
     ```bash
     docker exec -it price-transparency-etl-webserver-1 /bin/bash
     ```
     Replace `price-transparency-etl-webserver-1` with your Airflow container name.

2. **Navigate to Logs Directory**:
   - Once inside the container, navigate to the logs directory:
     ```bash
     cd /usr/local/airflow/logs
     ```

3. **View Logs**:
   - Use standard Unix commands (`ls`, `tail`, `cat`) to list and view log files.

4. **Exit Container**:
   - After viewing logs, exit the container:
     ```bash
     exit
     ```

### Connecting to PostgreSQL from Terminal

To connect to the PostgreSQL database from the terminal, use the following command:

```bash
docker exec -it price-transparency-etl-postgres-1 psql -U airflow
```

This command opens an interactive PostgreSQL shell, allowing you to execute SQL commands directly against the database.


## SQL Queries

- **Find Common Products Across All Branches**:
```sql
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
FROM common_items;
```

- **Find the Cheapest Basket**:
```sql
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
LIMIT 1;
```

- **Test Query for Top 30 Common Items Across Branches**:
```sql
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
ORDER BY TotalPrice ASC;
```


### Analytics and Insights

The `get_common_products_and_cheapest_basket` function in the ETL pipeline leverages these SQL queries to derive valuable insights:

- **Common Products**: It identifies which products are commonly available across the most branches, indicating popular or essential items.
  
- **Cheapest Basket**: It determines which branch offers the lowest combined price for the identified common products, providing insights into the most cost-effective shopping options.

These analytics help consumers compare prices effectively and make informed decisions when shopping at various branches of the supermarket chain.
---

