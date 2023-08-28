from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.operators.bash import BashOperator
from airflow.operators.python import ShortCircuitOperator
from bigeye_airflow.operators.create_metric_operator import CreateMetricOperator
from bigeye_airflow.operators.run_metrics_operator import RunMetricsOperator

BIGEYE_CONNECTION = 'bigeye_conn'

def test_table_has_no_alerts(**context) -> bool:
    if context['output']['failure']:
        return False
    else:
        return True
    

with DAG('ex_circuit_breaker', schedule_interval=None, start_date=days_ago(0), catchup=False) as dag:
    
    # TRANSFORM TABLE
    transform_table = BashOperator(
        task_id='transform_table',
        bash_command='echo I am transforming a very important table'
    )

    # CREATE BIGEYE METRICS
    create_metric = CreateMetricOperator(
        task_id='create_metrics',
        connection_id=BIGEYE_CONNECTION,
        warehouse_id=79,
        configuration=[
            {
                "schema_name": "TOOY_DEMO_DB.PROD_REPL",
                "table_name": "ORDERS",
                "column_name": "QUANTITY",
                "user_defined_metric_name": "Max Order Quantity",
                "metric_name": "MAX",
                "default_check_frequency_hours": 0,
                "notification_channels": {
                    "slack": "#data-alerts"
                },
                "thresholds": [
                    {
                        "constantThreshold": {
                            "bound": {
                                "boundType": "UPPER_BOUND_SIMPLE_BOUND_TYPE",
                                "value": 25
                            }
                        }
                    }
                ]
            },
        ],
        dag=dag
    )

    # EXECUTE BIGEYE METRICS
    run_metric = RunMetricsOperator(
        task_id='run_metrics',
        connection_id=BIGEYE_CONNECTION,
        metric_ids=create_metric.output,
        dag=dag
    )

    # VALIDATE NO BIGEYE METRICS ARE UNHEALTY
    circuit_breaker = ShortCircuitOperator(
        task_id='circuit_breaker',
        provide_context=True,
        python_callable=test_table_has_no_alerts,
        op_kwargs={'output': run_metric.output},
        dag=dag
    )

    # REFRESH DASHBOARD
    refresh_dashboard = BashOperator(
        task_id='refresh_dashboard',
        bash_command='echo I am refreshing a very important executive dashboard based on the table created in the first task'
    )

    transform_table >> create_metric >> run_metric >> circuit_breaker >> refresh_dashboard