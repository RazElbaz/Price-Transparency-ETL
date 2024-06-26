## Price Transparency ETL

### Overview

The **Price Transparency ETL** project aims to enhance price transparency in the retail sector by developing an efficient ETL (Extract, Transform, Load) pipeline. This project collects, processes, and analyzes price data from various supermarket companies, making it easier for consumers to compare prices and make informed purchasing decisions. By promoting competition and price transparency, this project contributes to the fight against the cost of living.

### Goals

- **Price Comparison**: Collect and aggregate price data from multiple retailers to allow consumers to compare prices.
- **Increase Competition**: Provide tools and insights that promote competitive pricing among retailers.
- **Support Legislation**: Comply with the food law (price transparency) requirements to ensure all collected data is accessible and transparent.
- **Data Integration**: Include various supermarket companies, starting with Shufersal and expanding to others.

### Features

- **Data Extraction**: Download price data from retailers' websites.
- **Data Transformation**: Clean and normalize data to ensure consistency and accuracy.
- **Data Loading**: Store transformed data in a PostgreSQL database for further analysis and reporting.
- **Scheduled Workflows**: Use Apache Airflow to schedule and manage ETL workflows, ensuring data is up-to-date.
- **Error Handling**: Implement robust error handling and logging to ensure the reliability of the ETL process.
