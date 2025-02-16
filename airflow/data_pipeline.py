from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

# define DAG
dag = DAG(
    "financial_data_pipeline",
    default_args={"start_date": days_ago(1)},
    schedule_interval="@daily",
)

# define task
data_fetch_task = BashOperator(
    task_id="fetch data from Alpha Vantage API",
    bash_command="python ./scripts/data_fetch.py",
    dag=dag,
)

data_clean_task = BashOperator(
    task_id="clean and transform data",
    bash_command="python ./scripts/data_clean_s3.py",
    dag=dag,
)

load_to_Redshift_task = BashOperator(
    task_id="upload data to Redshift",
    bash_command="python ./scripts/s3_to_redshift.py",
    dag=dag,
)

dbt_run_task = BashOperator(
    task_id="dbt_run",
    bash_command="cd ./financial_data_model && dbt run",
    dag=dag,
)

# Setting up task dependencies
data_cleaning_task >> load_to_redshift_task >> dbt_run_task