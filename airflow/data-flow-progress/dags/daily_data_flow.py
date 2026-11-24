from datetime import datetime, timezone

from airflow import DAG
from df_batch_progress import DAG_ID, TASK_ID, DataFlowBatchOperator


def identify_batch(dag_run, ti, **context):
    batch_id = (dag_run.conf or {}).get("batch_id")
    if isinstance(batch_id, bool) or not isinstance(batch_id, (str, int)):
        raise ValueError("Trigger config must contain a string or integer batch_id")
    if not str(batch_id).strip():
        raise ValueError("batch_id cannot be empty")
    ti.xcom_push(key="df_batch_id", value=batch_id)


with DAG(
    DAG_ID, start_date=datetime(2021, 1, 1, tzinfo=timezone.utc),
    schedule_interval=None, catchup=False,
) as dag:
    identify = DataFlowBatchOperator(task_id=TASK_ID, python_callable=identify_batch)
