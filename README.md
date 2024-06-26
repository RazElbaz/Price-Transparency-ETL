# Price Transparency ETL

## Overview

The **Price Transparency ETL** project aims to enhance price transparency in the retail sector by developing an efficient ETL (Extract, Transform, Load) pipeline. This project collects, processes, and analyzes price data from various supermarket companies, making it easier for consumers to compare prices and make informed purchasing decisions. By promoting competition and price transparency, this project contributes to the fight against the cost of living.

## Components

### ETL Workflow

The ETL pipeline consists of the following main tasks:

1. **Extract**: Downloads price data from the Shufersal website.
2. **Transform**: Parses XML files, transforms and standardizes the data.
3. **Load**: Inserts the transformed data into a PostgreSQL database.
4. **PostgreSQL Operations**: Creates necessary tables in PostgreSQL and handles data loading.

### Directory Structure

- **dags/**: Contains the Airflow DAG definition file (`etl_dag.py`).
- **ETL_functions/**:
  - **extract_functions.py**: Contains functions for extracting data from the Shufersal website.
  - **load_functions.py**: Includes functions for PostgreSQL operations, such as creating tables and loading data.
  - **transform_functions.py**: Contains functions for transforming XML data extracted from Shufersal.

### Docker Configuration

The project is containerized using Docker Compose, with the following services:

- **postgres**: PostgreSQL database (version 9.6) for storing transformed data.
- **webserver**: Airflow webserver container with local executor for managing and scheduling the ETL workflows.

## Getting Started

## DAG Details

The DAG `shufersal_branches_extraction` performs the following tasks:
1. **Create PostgreSQL Table**: Creates a table in PostgreSQL to store the pricing data.
2. **Extract Data**: Extracts pricing data from the Shufersal website.
3. **Transform Data**: Transforms the extracted XML data into a format suitable for loading into PostgreSQL.
4. **Load Data**: Loads the transformed data into the PostgreSQL database.
5. **Clear XML Files**: Clears the downloaded XML files to save space.
6. **Generate Report**: Generates a report identifying common products across all branches and the branch with the cheapest basket.


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
   - Enable the `shufersal_branches_extraction` DAG.
   - Trigger the DAG manually or wait for the scheduled interval (`*/30 * * * *` by default) to start the ETL process.

5. **Monitor and Manage DAG**:
   Use the Airflow UI to monitor DAG runs, view task logs, and manage workflow execution.

### Stopping the Services

To stop the running Docker Compose services, use the following command:

```bash
docker-compose down
```

This command stops and removes all the containers defined in the `docker-compose.yml` file.

### Connecting to PostgreSQL from Terminal

To connect to the PostgreSQL database from the terminal, use the following command:

```bash
docker exec -it price-transparency-etl-postgres-1 psql -U airflow
```

This command opens an interactive PostgreSQL shell, allowing you to execute SQL commands directly against the database.


## SQL Queries

The SQL queries used in the reporting task are:
- **Find Common Products Across All Branches**:
    ```sql
    SELECT ItemCode, ItemName
    FROM stores
    GROUP BY ItemCode, ItemName
    HAVING COUNT(DISTINCT StoreId) = (SELECT COUNT(DISTINCT StoreId) FROM stores)
    ```

- **Find the Cheapest Basket**:
    ```sql
    SELECT StoreId, SUM(ItemPrice) as TotalPrice
    FROM stores
    GROUP BY StoreId
    ORDER BY TotalPrice ASC
    LIMIT 1
    ```
    
---

