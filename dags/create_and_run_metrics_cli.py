from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from glob import glob


BIGCONFIG_FILES = str.join(" -ip ",[f for f in glob('/root/airflow/dbt/**/*.bigconfig.yml', recursive=True)])
BIGEYE_PROD_WID = 144


with DAG(
    'create_and_run_metrics_cli',
    schedule_interval=None,
    start_date=days_ago(0),
    catchup=False,
    default_args={
        "env": {
            "BIGEYE_API_CRED_FILE": "/root/airflow/bigeye/bigeye_conf.json"
        }
    }
) as dag:
    
    create_metrics = BashOperator(
        task_id="create_metrics",
        bash_command=f"bigeye bigconfig apply -nq -ip {BIGCONFIG_FILES}"
    )

    run_metrics = BashOperator(
        task_id="run_metrics",
        bash_command=f"bigeye catalog run-metrics -wid {BIGEYE_PROD_WID}"
    )
    
    create_metrics >> run_metrics