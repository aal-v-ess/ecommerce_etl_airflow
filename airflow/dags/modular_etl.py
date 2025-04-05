from datetime import datetime, timedelta
import os
import random
from typing import Optional

import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.models import Variable


# Define default arguments for the DAG
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=1),
}

# Create the DAG
dag = DAG(
    'amazon_product_etl_csv',
    default_args=default_args,
    description='ETL pipeline for Amazon product data (CSV to CSV)',
    schedule_interval='0 2 * * *',  # Run daily at 2 AM
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['amazon', 'product', 'etl', 'csv'],
    max_active_runs=1,
    doc_md="""
    # Amazon Product Data ETL Pipeline (CSV to CSV)
    
    This DAG processes raw Amazon product data from a CSV file and writes the processed data to a new CSV file.
    
    ## Process Flow:
    1. Check for input file
    2. Extract data from input CSV
    3. Multiple transformation steps:
       - Filter for US locale
       - Keep only required columns
       - Parse categories
       - Parse review ratings and counts
       - Parse prices
       - Clean and convert data types
    4. Load data to output CSV
    5. Perform data quality checks
    """
)

# Define utility functions
def parse_category(category: str) -> list[str]:
    """Parse the category string and return the first category.

    Args:
        category: String containing the category list

    Returns:
        String containing the first category, or empty list if parsing fails
    """
    if isinstance(category, list) and pd.isna(category).any():
        return []
    elif not isinstance(category, list) and pd.isna(category):
        return []

    try:
        keep_num_categories = 1 if random.random() < 0.9 else 2
        return [c.strip() for c in category][:keep_num_categories]
    except (ValueError, IndexError):
        return []


def parse_review_rating(stars: str) -> Optional[float]:
    """Parse the stars rating from a string to a float value between 0 and 5.

    Args:
        stars: String containing the star rating (e.g., "4.2 out of 5 stars")

    Returns:
        Float value between 0 and 5, or -1.0 if parsing fails
    """
    if pd.isna(stars):
        return -1.0
    stars_str = str(stars).replace(",", ".")  # Handle European number format
    try:
        return float(stars_str.split()[0])
    except (ValueError, IndexError):
        return -1.0


def parse_review_count(ratings: str) -> Optional[int]:
    """Parse the number of ratings from a string to an integer value.

    Args:
        ratings: String containing the number of ratings (e.g., "1,116 ratings")

    Returns:
        Int value representing the number of ratings, or 0 if parsing fails
    """
    if pd.isna(ratings):
        return 0
    try:
        # Remove commas and get first number
        ratings_str = str(ratings).split()[0].replace(",.", "")
        return int(ratings_str)
    except (ValueError, IndexError):
        return 0


def parse_price(price: str) -> Optional[float]:
    """Parse the price from a string to a float value.

    Args:
        price: String containing the price (e.g., "$9.99", "25,63€")

    Returns:
        Float value representing the price, or None if parsing fails or price is NaN
    """
    if pd.isna(price):
        return None

    try:
        # Remove currency symbols and convert to float
        price_str = str(price).replace("$", "").replace("€", "").replace(",", ".")
        return min(float(price_str), 1000.0)
    except ValueError:
        return None
    

# Define task functions
def extract_data(**context):
    """Extract data from the source CSV file"""
    # Get the input file path from Airflow Variables or use a default
    input_path = Variable.get("amazon_data_input_path", 
                             "/opt/airflow/data/raw/amazon_products.json")
    
    try:
        # Read the CSV file
        df = pd.read_json(input_path, lines=True)
        
        # Store metrics
        context['ti'].xcom_push(key='raw_data_rows', value=len(df))
        
        # Save to a temporary location for the next task
        temp_path = os.path.join(
            Variable.get("temp_data_path", "/opt/airflow/data/temp"),
            "extracted_data.csv"
        )
        
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(temp_path), exist_ok=True)
        
        # Save to CSV
        df.to_csv(temp_path, index=False)
        
        return temp_path
    except Exception as e:
        context['ti'].xcom_push(key='extract_error', value=str(e))
        raise


def filter_us_locale(**context):
    """Filter data for US locale only"""
    # Get the path from the previous task
    temp_path = context['ti'].xcom_pull(task_ids='extract_data')
    
    # Read the data
    df = pd.read_csv(temp_path)
    
    # Filter for US locale
    df = df[df["locale"] == "us"]
    
    # Save the filtered data
    filtered_path = os.path.join(
        Variable.get("temp_data_path", "/opt/airflow/data/temp"),
        "us_filtered_data.csv"
    )
    
    # Save to CSV
    df.to_csv(filtered_path, index=False)
    
    # Push metrics
    context['ti'].xcom_push(key='us_filtered_rows', value=len(df))
    
    return filtered_path


def select_columns(**context):
    """Keep only required columns"""
    # Get the path from the previous task
    filtered_path = context['ti'].xcom_pull(task_ids='filter_us_locale')
    
    # Read the data
    df = pd.read_csv(filtered_path)
    
    # Keep only required columns
    columns_to_keep = [
        "asin",
        "type",
        "category",
        "title",
        "description",
        "stars",
        "ratings",
        "price",
    ]
    df_processed = df[columns_to_keep]
    
    # Save the processed data
    columns_path = os.path.join(
        Variable.get("temp_data_path", "/opt/airflow/data/temp"),
        "columns_selected_data.csv"
    )
    
    # Save to CSV
    df_processed.to_csv(columns_path, index=False)
    
    return columns_path


def transform_categories(**context):
    """Transform category data"""
    # Set random seed for reproducibility
    random.seed(6)
    
    # Get the path from the previous task
    columns_path = context['ti'].xcom_pull(task_ids='select_columns')
    
    # Read the data
    df = pd.read_csv(columns_path)
    
    # Transform categories
    df["category"] = df["category"].apply(parse_category)
    
    # Convert the category list to string for CSV storage
    df["category_str"] = df["category"].apply(lambda x: "|".join(x) if x else "")
    df = df.drop(columns=["category"])
    
    # Save the transformed data
    categories_path = os.path.join(
        Variable.get("temp_data_path", "/opt/airflow/data/temp"),
        "categories_transformed_data.csv"
    )
    
    # Save to CSV
    df.to_csv(categories_path, index=False)
    
    return categories_path


def transform_reviews(**context):
    """Transform review ratings and counts"""
    # Get the path from the previous task
    categories_path = context['ti'].xcom_pull(task_ids='transform_categories')
    
    # Read the data
    df = pd.read_csv(categories_path)
    
    # Transform review data
    df["review_rating"] = df["stars"].apply(parse_review_rating)
    df["review_count"] = df["ratings"].apply(parse_review_count)
    
    # Drop original stars and ratings columns
    df = df.drop(columns=["stars", "ratings"])
    
    # Save the transformed data
    reviews_path = os.path.join(
        Variable.get("temp_data_path", "/opt/airflow/data/temp"),
        "reviews_transformed_data.csv"
    )
    
    # Save to CSV
    df.to_csv(reviews_path, index=False)
    
    return reviews_path


def transform_prices(**context):
    """Transform price data"""
    # Get the path from the previous task
    reviews_path = context['ti'].xcom_pull(task_ids='transform_reviews')
    
    # Read the data
    df = pd.read_csv(reviews_path)
    
    # Transform prices
    df["price"] = df["price"].apply(parse_price)
    
    # Drop rows with missing prices
    df = df.dropna(subset=["price"])
    
    # Save the transformed data
    prices_path = os.path.join(
        Variable.get("temp_data_path", "/opt/airflow/data/temp"),
        "prices_transformed_data.csv"
    )
    
    # Save to CSV
    df.to_csv(prices_path, index=False)
    
    # Push metrics
    context['ti'].xcom_push(key='after_price_transform_rows', value=len(df))
    
    return prices_path


def finalize_data_types(**context):
    """Finalize data types for all columns"""
    # Get the path from the previous task
    prices_path = context['ti'].xcom_pull(task_ids='transform_prices')
    
    # Read the data
    df = pd.read_csv(prices_path)
    
    # Type conversions
    df = df.astype({
        "asin": str,
        "type": str,
        "title": str,
        "description": str,
        "review_rating": float,
        "review_count": int,
        "price": float,
    })
    
    # Save the transformed data
    final_transform_path = os.path.join(
        Variable.get("temp_data_path", "/opt/airflow/data/temp"),
        "final_transformed_data.csv"
    )
    
    # Save to CSV
    df.to_csv(final_transform_path, index=False)
    
    # Push metrics
    context['ti'].xcom_push(key='transformed_data_rows', value=len(df))
    
    return final_transform_path


def load_to_csv(**context):
    """Load the transformed data to a CSV file"""
    # Get the path from the previous task
    final_transform_path = context['ti'].xcom_pull(task_ids='finalize_data_types')
    
    # Read the transformed data
    df = pd.read_csv(final_transform_path)
    
    # Get the output file path
    output_path = Variable.get(
        "amazon_data_output_path", 
        "/opt/airflow/data/processed/amazon_products_processed.csv"
    )
    
    # Create directory if it doesn't exist
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    # Save to CSV
    df.to_csv(output_path, index=False)
    
    # Push metrics
    context['ti'].xcom_push(key='loaded_rows', value=len(df))
    
    return output_path


def run_data_quality_checks(**context):
    """Run data quality checks on the processed data"""
    # Get the output file path
    output_path = context['ti'].xcom_pull(task_ids='load_to_csv')
    
    # Read the output file
    df = pd.read_csv(output_path)
    
    # Check for duplicates
    duplicate_count = df["asin"].duplicated().sum()
    
    # Check for invalid ratings (input for NaN is -1 currently)
    invalid_ratings = ((df["review_rating"] < -1) | (df["review_rating"] > 5)).sum()
    
    # Check for negative prices out of scope 
    negative_prices = (df["price"] < -1).sum()
    
    # Store results
    dq_results = {
        'duplicate_asin_count': int(duplicate_count),
        'invalid_ratings_count': int(invalid_ratings),
        'negative_prices_count': int(negative_prices),
        'passed': duplicate_count == 0 and invalid_ratings == 0 and negative_prices == 0
    }
    
    context['ti'].xcom_push(key='dq_results', value=dq_results)
    
    if not dq_results['passed']:
        raise ValueError(f"Data quality checks failed: {dq_results}")
    
    return dq_results


# Define the operators/tasks
check_for_input_file = FileSensor(
    task_id='check_for_input_file',
    filepath=Variable.get(
        "amazon_data_input_path", 
        "/opt/airflow/data/raw/amazon_products.json"
    ),
    fs_conn_id='fs_default',
    poke_interval=300,  # 5 minutes
    timeout=60 * 60 * 12,  # 12 hours
    mode='poke',
    dag=dag
)

extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    provide_context=True,
    dag=dag
)

filter_us_task = PythonOperator(
    task_id='filter_us_locale',
    python_callable=filter_us_locale,
    provide_context=True,
    dag=dag
)

select_columns_task = PythonOperator(
    task_id='select_columns',
    python_callable=select_columns,
    provide_context=True,
    dag=dag
)

transform_categories_task = PythonOperator(
    task_id='transform_categories',
    python_callable=transform_categories,
    provide_context=True,
    dag=dag
)

transform_reviews_task = PythonOperator(
    task_id='transform_reviews',
    python_callable=transform_reviews,
    provide_context=True,
    dag=dag
)

transform_prices_task = PythonOperator(
    task_id='transform_prices',
    python_callable=transform_prices,
    provide_context=True,
    dag=dag
)

finalize_data_types_task = PythonOperator(
    task_id='finalize_data_types',
    python_callable=finalize_data_types,
    provide_context=True,
    dag=dag
)

load_task = PythonOperator(
    task_id='load_to_csv',
    python_callable=load_to_csv,
    provide_context=True,
    dag=dag
)

data_quality_task = PythonOperator(
    task_id='run_data_quality_checks',
    python_callable=run_data_quality_checks,
    provide_context=True,
    dag=dag
)

# Define the task dependencies
check_for_input_file >> extract_task >> filter_us_task >> select_columns_task >> \
transform_categories_task >> transform_reviews_task >> transform_prices_task >> \
finalize_data_types_task >> load_task >> data_quality_task