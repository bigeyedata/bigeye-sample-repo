from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.operators.bash import BashOperator
from airflow.operators.python import ShortCircuitOperator

from typing import List

import bigeye_sdk.authentication.api_authentication as bigeye_auth
import bigeye_sdk.client.datawatch_client as bigeye_client
from bigeye_sdk.generated.com.bigeye.models.generated import MetricInfo, MetricRunStatus


BIGEYE_CONNECTION = 'bigeye_conn'

def test_collection_has_no_alerts(**context) -> bool:
    api_auth = bigeye_auth.ApiAuthConf.load_from_file('/root/airflow/bigeye/bigeye_conf.json')
    client = bigeye_client.datawatch_client_factory(api_auth)

    collection_metrics: List[int] = client.get_collection(collection_id=context['collection_id']).collection.metric_ids

    metric_infos: List[MetricInfo] = client.run_metric_batch(metric_ids=collection_metrics).metric_infos

    if any(mi.status is not MetricRunStatus.METRIC_RUN_STATUS_OK for mi in metric_infos):
        return False
    else:
        return True


with DAG('ex_circuit_breaker_collection', schedule_interval=None, start_date=days_ago(0), catchup=False) as dag:
    
    # TRANSFORM TABLE
    transform_table = BashOperator(
        task_id='transform_table',
        bash_command='echo I am transforming a very important table'
    )

    # VALIDATE NO BIGEYE METRICS ARE UNHEALTY
    circuit_breaker = ShortCircuitOperator(
        task_id='circuit_breaker',
        provide_context=True,
        python_callable=test_collection_has_no_alerts,
        op_kwargs={'collection_id': 96},
        dag=dag
    )

    # REFRESH DASHBOARD
    refresh_dashboard = BashOperator(
        task_id='refresh_dashboard',
        bash_command='echo I am refreshing a very important executive dashboard based on the table created in the first task'
    )

    transform_table >> circuit_breaker >> refresh_dashboard