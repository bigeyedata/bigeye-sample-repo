from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago


DBT_PROJECT_DIR = "/root/airflow/dbt"


with DAG(
    "dbt_basic_dag",
    start_date=days_ago(0),
    description="A sample Airflow DAG to invoke dbt runs using a BashOperator",
    schedule_interval=None,
    catchup=False,
    default_args={
        "env": {
            "DBT_ENV_SECRET_SNOWFLAKE_HOST":        "{{ conn.snowflake_dev_conn.account }}",
            "DBT_ENV_SECRET_SNOWFLAKE_WAREHOUSE":   "{{ conn.snowflake_dev_conn.warehouse }}",
            "DBT_ENV_SECRET_SNOWFLAKE_USER":        "{{ conn.snowflake_dev_conn.login }}",
            "DBT_ENV_SECRET_SNOWFLAKE_PASSWORD":    "{{ conn.snowflake_dev_conn.password }}"
        }
    },
) as dag:
    
    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command=f"dbt seed --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"dbt run --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"dbt test --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}",
    )

    dbt_seed >> dbt_run >> dbt_test