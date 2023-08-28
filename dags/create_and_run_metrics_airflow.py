from airflow import DAG
from airflow.utils.dates import days_ago

from bigeye_airflow.operators.create_metric_operator import CreateMetricOperator
from bigeye_airflow.operators.run_metrics_operator import RunMetricsOperator

BIGEYE_PROD_WID = 144
BIGEYE_CONNECTION = 'bigeye_conn'

with DAG(
    'create_and_run_metrics_airflow',
    schedule_interval=None,
    start_date=days_ago(0),
    catchup=False,
) as dag:
    create_metric = CreateMetricOperator(
        task_id='create_metrics',
        connection_id=BIGEYE_CONNECTION,
        warehouse_id=BIGEYE_PROD_WID,
        configuration=[
            {
                "schema_name": "TOOY_DEMO_DB.PROD_REPL",
                "table_name": "ORDERS",
                "column_name": "QUANTITY",
                "user_defined_metric_name": "Average Order Quantity",
                "metric_name": "AVERAGE",
                "default_check_frequency_hours": 0,
                "notification_channels": {
                    "slack": "#data-alerts"
                }
            },
            {
                "schema_name": "TOOY_DEMO_DB.PROD_REPL",
                "table_name": "ORDERS",
                "column_name": "PRODUCT_TYPE",
                "user_defined_metric_name": "Value in List",
                "metric_name": "PERCENT_VALUE_IN_LIST",
                "default_check_frequency_hours": 0,
                "notification_channels": {
                    "slack": "#data-alerts"
                }
             }
        ],
        dag=dag
    )
    run_metric = RunMetricsOperator(
        task_id='run_metrics',
        connection_id=BIGEYE_CONNECTION,
        metric_ids=create_metric.output,
        dag=dag
    )

    create_metric >> run_metric