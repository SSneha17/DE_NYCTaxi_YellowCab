
## Project Overview
This project demonstrates the end-to-end process of building a **Data Engineering pipeline** using the **Medallion Architecture** on Azure, with the NYC Yellow Cab 2024 dataset. The goal is to process and transform the data across different layers, making it ready for analysis and reporting.

### Key Steps:
1. **Bronze Layer**: Raw data is ingested from Parquet files and stored in the Bronze layer.
2. **Silver Layer**: Data is cleaned and transformed into a more structured form using PySpark on **Azure Databricks**.
3. **Gold Layer**: The processed data is stored in optimized **Delta Tables** for analysis and reporting.

## Technologies Used
This project leverages the following technologies:
- **Azure Data Factory**: Used to orchestrate data pipeline tasks and move data across different layers.
- **Azure Databricks**: Provides a scalable cloud-based environment for processing data using **PySpark**.
- **PySpark**: Used for data transformations and loading between layers (Bronze, Silver, and Gold).
- **Delta Lake**: Optimized storage format used in the Gold layer to enable ACID transactions and fast queries.
- **Parquet**: Columnar storage format used for the Bronze and Silver layers.
- **Azure Blob Storage**: Used for storing raw and processed datasets in Parquet format.

## Data Sources
The primary data source for this project is the [NYC Yellow Taxi Dataset](https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page). This dataset contains detailed trip information for Yellow Cabs in New York City in 2024, including:
- Trip duration
- Pickup and drop-off locations
- Fares and payments
- Passenger count
- Timestamp details

The data is stored in Parquet format for easy ingestion and processing.

## Architecture and Data Flow
![image](https://github.com/user-attachments/assets/0863acff-931c-4679-84ce-58ad02cdf054)

This project follows the **Medallion Architecture**, where data is processed in stages across three layers:

### 1. **Bronze Layer** (Raw Data):
- Raw Parquet files from the NYC Yellow Cab 2024 dataset are ingested.
- The data is stored in the Bronze layer with minimal transformation.

### 2. **Silver Layer** (Cleaned and Transformed Data):
- Using **PySpark** in **Azure Databricks**, the data is cleaned and enriched. Common transformations include:
  - Handling missing values
  - Standardizing columns
  - Creating new features for analysis
- Data is stored in the Silver layer in Parquet format.

### 3. **Gold Layer** (Optimized Data for Analysis):
- In this final layer, the cleaned and enriched data is further aggregated and optimized for analytics.
- The data is stored in **Delta Tables**, providing benefits such as ACID transactions, schema enforcement, and fast queries.

### **Azure Data Factory** Pipeline:
- Azure Data Factory is used to automate the movement of data through the pipeline from the Bronze layer to the Silver and Gold layers.
- Tasks such as data ingestion, transformation, and writing to storage are orchestrated using ADF pipelines.

