from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import os
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
import json
import numpy as np
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

default_args = {
    "owner": 'Pramita',
    "retries": 1,
    "retry_delay": timedelta(seconds=10)
}

def rename_file(downloaded_file_name, new_name):
    # downloaded_file_name : /tmp/ariflow_askajkja
    downloaded_file_path = '/'.join(downloaded_file_name.split('/')[:-1])
    new_name_for_file = f'{downloaded_file_path}/{new_name}'
    os.rename(src=downloaded_file_name, dst=new_name_for_file)

def download_csv_from_s3() -> None:
    files = ['customer.csv', 'item.csv', 'orders.csv', 'restaurant.csv', 'Status.csv']
    hook = S3Hook('my-aws-conn')
    for file in files:
        file_name = hook.download_file(
            key=file,
            bucket_name='food-delivery-system'
        )
        print(file_name)
        rename_file(file_name, file)
    return True

def merge_data():
    customer_df = pd.read_csv('/tmp/customer.csv')
    item_df = pd.read_csv('/tmp/item.csv')
    orders_df = pd.read_csv('/tmp/orders.csv')
    restaurant_df = pd.read_csv('/tmp/restaurant.csv')
    status_df = pd.read_csv('/tmp/Status.csv')

    # merging dfs

    orders_customer_df = pd.merge(orders_df, customer_df, how="left", on="customerid")
    orders_customer_items_df = pd.merge(orders_customer_df, item_df, how="left", on="itemid")
    orders_customer_items_restaurant_df = pd.merge(orders_customer_items_df, restaurant_df.drop(columns=['itemid']), how="left", on="restaurantid")
    orders_customer_items_restaurant_status_df = pd.merge(orders_customer_items_restaurant_df, status_df, how="left", on="orderStatusId")
    
    orders_customer_items_restaurant_status_df.to_csv("/tmp/merged_data.csv",index=None)
    

def transform_data():
    orders_customer_items_restaurant_status_df = pd.read_csv('/tmp/merged_data.csv')

    ### TRANSFORM HERE
    orders_customer_items_restaurant_status_df['isTakingOrders'] = np.where(orders_customer_items_restaurant_status_df['isTakingOrders']== False, 0, 1)
    orders_customer_items_restaurant_status_df["contact_no"] = orders_customer_items_restaurant_status_df["contact_no"].str.replace("-", "")
    orders_customer_items_restaurant_status_df["zipcode"] = orders_customer_items_restaurant_status_df['address'].apply(lambda x: x.split(",")[2])
    orders_customer_items_restaurant_status_df["state"] = orders_customer_items_restaurant_status_df['address'].apply(lambda x: x.split(",")[0])

    orders_customer_items_restaurant_status_df.to_csv("/tmp/merged_data.csv",index=None)

with DAG(
    dag_id = "food_delivery_management_dag",
    description = "food_delivery_management_dag",
    start_date =datetime(2024,5,15,2),
    schedule_interval = None,
    default_args=default_args
) as dag:
    task_download_csv_from_s3 = PythonOperator(
        task_id = "download_csv_from_s3",
        python_callable=download_csv_from_s3
    )

    task_merge_data = PythonOperator(
        task_id = "merge_data",
        python_callable=merge_data
    )

    task_transform_data = PythonOperator(
        task_id = "transform_data",
        python_callable=transform_data
    )

    task_push_merged_data_to_gcs = LocalFilesystemToGCSOperator(
        task_id ="upload_csv_file",
        bucket='food-delivery-management',
        src='/tmp/merged_data.csv',
        dst='merged_data.csv',
        gcp_conn_id="my-gcp-conn"
    )

    task_push_data_from_gcs_to_bigquery = GCSToBigQueryOperator(
    task_id='push_data_from_gcs_to_bigquery',
    bucket='food-delivery-management',
    source_objects=['merged_data.csv'],
    destination_project_dataset_table=f'data-engineering-424902:food_delivery_management.food-delivery-merged-table',
    write_disposition='WRITE_TRUNCATE',
    source_format='csv',
    allow_quoted_newlines='true',
    gcp_conn_id="my-gcp-conn",
    skip_leading_rows=1
)


    task_download_csv_from_s3 >> task_merge_data >> task_transform_data >> task_push_merged_data_to_gcs >> task_push_data_from_gcs_to_bigquery
