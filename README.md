# Food Delivery Management System

## Project Overview
This project utilizes an ETL pipeline built with Apache Airflow to manage and optimize data processing from multiple AWS S3 sources for a food delivery management system. The data is transformed and loaded into Google BigQuery, where it is used to generate dynamic reports with Power BI.

## Technologies Used
- **Python**: For scripting and data manipulation.
- **Apache Airflow**: Orchestrates the workflow of data pipeline jobs.
- **AWS S3**: Stores raw data files.
- **Google BigQuery**: Hosts the data warehouse where transformed data is loaded.
- **Power BI**: Utilized for generating and optimizing dynamic reports.

## Features
- Seamless extraction of data from five distinct AWS S3 sources.
- Data preprocessing includes cleaning, normalization, and transformation of over 50,000 records.
- Integration of dynamic reporting tools to reduce report preparation time by 30%.

## System Architecture

![image](https://github.com/Pramita0410/food-delivery-data-analysis/assets/114774760/248ae756-3c1d-4792-9a0c-eeda38b02fb9)
The above diagram illustrates the workflow of the data from extraction to reporting.

## DAG Configuration
The DAG `food_delivery_management_dag` manages the pipeline with tasks for downloading, merging, transforming, and loading data.

### Key DAG Tasks
- `download_csv_from_s3`: Downloads files from AWS S3.
- `merge_data`: Merges various datasets into a single CSV file.
- `transform_data`: Performs data cleaning and transformation operations.
- `upload_csv_file`: Uploads the merged CSV to Google Cloud Storage.
- `push_data_from_gcs_to_bigquery`: Loads the data into BigQuery from Google Cloud Storage.

### S3

![image](https://github.com/Pramita0410/food-delivery-data-analysis/assets/114774760/871f4d2f-a368-4206-ad4e-439865f4ab2f)

### Airflow DAG

![image](https://github.com/Pramita0410/food-delivery-data-analysis/assets/114774760/faffc78a-ca4c-42cc-97e5-c2b03237c7f4)

### Google Big Query

![image](https://github.com/Pramita0410/food-delivery-data-analysis/assets/114774760/d0383e7a-9e36-4a48-a94a-7cb82e27b6e4)

