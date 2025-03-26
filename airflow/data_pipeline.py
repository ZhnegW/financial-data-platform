from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from airflow.operators.email_operator import EmailOperator

# define DAG
dag = DAG(
    "financial_data_pipeline",
    default_args={"start_date": days_ago(1)},
    schedule_interval="@daily",
)

# define task
data_ingest_task = BashOperator(
    task_id="fetch data from Alpha Vantage API",
    bash_command="python ./scripts/data_ingest.py",
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
    bash_command="cd ./financial_data_model && dbt run --models marts.core+ && dbt test",
    dag=dag,
)

data_analysis_task = BashOperator(
    task_id="data analysis",
    bash_command="python ./scripts/data_analysis.py",
    dag=dag,
)

data_fetch_task = BashOperator(
    task_id="fetch data from Alpha Vantage API",
    bash_command="python ./scripts/fetch_data_v3.py",
    dag=dag,
)

data_transformation_task = BashOperator(
    task_id="process data",
    bash_command="python ./scripts/data_transformation_v3.py",
    dag=dag,
)

data_load_task = BashOperator(
    task_id="load processed data to redshift",
    bash_command="python ./scripts/Load_to_redshift_v3.py",
    dag=dag,
)

email_task = EmailOperator(
    task_id="send_email",
    to="your_email@example.com",
    subject="Airflow Task Failed",
    html_content="<p>The task {{ task_id }} failed.</p>",
    dag=dag,
    trigger_rule="one_failed",
)

# Setting up task dependencies
data_fetch_task >> data_transformation_task >> data_load_task >> dbt_run_task >> email_task