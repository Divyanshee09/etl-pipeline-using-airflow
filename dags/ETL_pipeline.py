from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd

# Task 1: Extract Data from PostgreSQL
def extract_data(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id="my_postgres_conn")
    engine = pg_hook.get_sqlalchemy_engine()
    print("Connected to POSTGRES Successfully ✅ !!!")

    # Read tables from PostgreSQL
    articles = pd.read_sql("SELECT * FROM articles LIMIT 1000", con=engine)
    customers = pd.read_sql("SELECT * FROM customers LIMIT 1000", con=engine)
    transactions = pd.read_sql("SELECT * FROM transactions LIMIT 1000", con=engine)
    print("Read table from Postgres Successfully ✅ !!!")

    # Store data in temporary tables
    articles.to_sql("temp_articles", con=engine, if_exists="replace", index=False)
    customers.to_sql("temp_customers", con=engine, if_exists="replace", index=False)
    transactions.to_sql("temp_transactions", con=engine, if_exists="replace", index=False)
    print("Stored data in temp table POSTGRES Successfully ✅ !!!")

    # Pass table names via XCom
    ti = kwargs['ti']
    ti.xcom_push(key="table_names", value=["temp_articles", "temp_customers", "temp_transactions"])

# Task 2: Transform Data
def transform_data(**kwargs):
    ti = kwargs['ti']
    table_names = ti.xcom_pull(task_ids="extract_data", key="table_names")

    if not table_names:
        raise ValueError("No table names found in XCom!")

    temp_articles, temp_customers, temp_transactions = table_names

    pg_hook = PostgresHook(postgres_conn_id="my_postgres_conn")
    engine = pg_hook.get_sqlalchemy_engine()

    # Read temporary tables
    articles = pd.read_sql(f"SELECT * FROM {temp_articles}", con=engine)
    customers = pd.read_sql(f"SELECT * FROM {temp_customers}", con=engine)
    transactions = pd.read_sql(f"SELECT * FROM {temp_transactions}", con=engine)
    # Convert article_id to int64 in both DataFrames before merging
    articles['article_id'] = articles['article_id'].astype('int64')
    transactions['article_id'] = transactions['article_id'].astype('int64')
    customers['customer_id'] = customers['customer_id'].astype('str')  # Convert to string to ensure consistent type
    transactions['customer_id'] = transactions['customer_id'].astype('str')
    

    # Data Cleaning
    # Articles Table

    articles = articles.rename(columns={
        'product_code': 'Product Code',
        'prod_name': 'Product Name',
        'product_type_name': 'Product Type',
        'product_group_name': 'Product Group',
        'colour_group_name': 'Color',
        'department_name': 'Department',
        'detail_desc': 'Description',
        'graphical_appearance_name': 'Graphical Appearance',
        'index_name': 'Index',
        'index_group_name': 'Index Group',
        'section_name': 'Section',
        'garment_group_name': 'Garment Group'
    })
    
    articles = articles.drop([
        'product_type_no', 'graphical_appearance_no', 'colour_group_code',
        'perceived_colour_value_id', 'perceived_colour_master_id', 'department_no',
        'index_code', 'index_group_no', 'section_no', 'garment_group_no',
        'perceived_colour_value_name', 'perceived_colour_master_name'
    ], axis=1)
    
    articles = articles.drop_duplicates()
    
    # Customers Table
    customers = customers.drop_duplicates()
    customers['fn'] = pd.to_numeric(customers['fn'], errors='coerce').fillna(0).astype(int)
    customers['active'] = pd.to_numeric(customers['active'], errors='coerce').fillna(0).astype(int)
    customers['club_member_status'] = customers['club_member_status'].fillna("Unknown")
    customers['fashion_news_frequency'] = customers['fashion_news_frequency'].fillna("Unknown")
    customers = customers.drop(['postal_code'], axis=1)
    
    # Feature Engineering
    customers['Age_Group'] = pd.cut(customers['age'], bins=[0, 18, 35, 50, 65, 100],
                                    labels=['<18', '18-35', '36-50', '51-65', '65+'])
    
    customers["club_member_status"] = customers["club_member_status"].replace({'' : "UNKNOWN"})
    customers["club_member_status"] = customers["club_member_status"].map({
        "ACTIVE": 1, "LEFT CLUB": 0, "PRE-CREATE": 2, "UNKNOWN": 3
    })
    
    # Transactions Table
    transactions['article_id'] = transactions['article_id']
    transactions['t_dat'] = pd.to_datetime(transactions['t_dat'])
    transactions['price'] = transactions['price'].astype(float)
    transactions["Sales Channel"] = transactions["sales_channel_id"].replace({1: "Online", 2: "Offline"})
    transactions = transactions.drop(columns=["sales_channel_id"])
    transactions = transactions.rename(columns={
        't_dat': 'Transaction Date',
        'price': 'Price'
    })
    transactions = transactions.drop_duplicates()
    
    def get_season(date):
        month = pd.to_datetime(date).month
        if month in [3, 4, 5]:
            return "Spring"
        elif month in [6, 7, 8]:
            return "Summer"
        elif month in [9, 10, 11]:
            return "Autumn"
        else:
            return "Winter"
    
    transactions["season"] = transactions["Transaction Date"].apply(get_season)
    
     # Store transformed DataFrames in temporary tables for the load phase
    articles.to_sql("temp_transformed_articles", con=engine, if_exists="replace", index=False)
    customers.to_sql("temp_transformed_customers", con=engine, if_exists="replace", index=False)
    transactions.to_sql("temp_transformed_transactions", con=engine, if_exists="replace", index=False)
    
    # Pass temporary table names via XCom
    ti.xcom_push(key="transformed_tables", value=[
        "temp_transformed_articles",
        "temp_transformed_customers",
        "temp_transformed_transactions"
    ])
    print("Data Transformed Successfully ✅ !!!")

# Task 3: Load Data with Optimizations
def load_data(**kwargs):
    try:
        ti = kwargs['ti']
        transformed_tables = ti.xcom_pull(task_ids="transform_data", key="transformed_tables")

        if not transformed_tables:
            raise ValueError("No transformed table names found in XCom!")

        pg_hook = PostgresHook(postgres_conn_id="my_postgres_conn")
        engine = pg_hook.get_sqlalchemy_engine()

        # Define batch size for processing
        BATCH_SIZE = 5000

        # Define mapping of temporary to final tables
        table_mapping = {
            "temp_transformed_articles": "transformed_articles",
            "temp_transformed_customers": "transformed_customers",
            "temp_transformed_transactions": "transformed_transactions"
        }

        # Process each table in batches
        for temp_table, final_table in table_mapping.items():
            # Get total count of records
            count_query = f"SELECT COUNT(*) FROM {temp_table}"
            total_records = pd.read_sql(count_query, engine).iloc[0, 0]
            
            # Calculate number of batches
            num_batches = (total_records // BATCH_SIZE) + 1
            
            # Process in batches
            for batch in range(num_batches):
                offset = batch * BATCH_SIZE
                batch_query = f"""
                    SELECT * FROM {temp_table} 
                    LIMIT {BATCH_SIZE} 
                    OFFSET {offset}
                """
                
                df_batch = pd.read_sql(batch_query, engine)
                
                # For first batch, replace the existing table
                # For subsequent batches, append to the table
                if batch == 0:
                    df_batch.to_sql(final_table, con=engine, if_exists="replace", index=False)
                else:
                    df_batch.to_sql(final_table, con=engine, if_exists="append", index=False)
                
                print(f"✅ Processed batch {batch + 1}/{num_batches} for {final_table}")
                
                # Clear memory
                del df_batch
                
        print("✅ All data loaded successfully to PostgreSQL")
        
    except Exception as e:
        print(f"❌ Error in load_data: {str(e)}")
        raise
    
# Define DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 2, 14),
    'retries': 1
}

with DAG(
    dag_id= 'Etl_Pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:
    
    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data,
        dag=dag
    )
    
    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        dag=dag
    )
    
    load_task = PythonOperator(
        task_id='load_data',
        python_callable=load_data,
        dag=dag
    )
    
    # Set task dependencies
    extract_task >> transform_task >> load_task
    