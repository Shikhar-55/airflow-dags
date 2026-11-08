"""
medallion_bronze_dag.py   (DAG 1 of 3)

Every 2 hours: create a VM, run bronze.py on it, delete the VM,
then trigger the Silver DAG.

    create_resource_group >> create_network >> create_vm >> run_bronze >> delete_vm >> trigger_silver
"""

import os
import sys
from datetime import datetime, timedelta

from airflow.sdk import DAG, Variable
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

# The DAGs are synced into a subfolder of the dags folder, so add this file's own
# folder to the import path before importing the shared helper next to it
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import medallion_utils as vm

default_args = {
    "retries": 2,                                   # try each task up to 3 times in total
    "retry_delay": timedelta(minutes=5),
    "email": [Variable.get("ALERT_EMAIL")],         # where failure emails go
    "email_on_failure": True,                       # send one when a task has used up its retries
    "email_on_retry": False,
}

with DAG(
    dag_id="medallion_bronze",
    schedule="0 */2 * * *",                         # every 2 hours, on the hour
    start_date=datetime(2026, 1, 1),
    catchup=False,                                  # the watermark does the catching up, not Airflow
    max_active_runs=1,
    default_args=default_args,
):
    rg = PythonOperator(task_id="create_resource_group", python_callable=vm.create_resource_group)
    network = PythonOperator(task_id="create_network", python_callable=vm.create_network)
    create = PythonOperator(task_id="create_vm", python_callable=vm.create_vm)
    run = PythonOperator(task_id="run_bronze", python_callable=vm.run_layer, op_args=["bronze"])
    delete = PythonOperator(task_id="delete_vm", python_callable=vm.delete_vm,
                            trigger_rule="all_done")                # always clean up, even on failure
    trigger_silver = TriggerDagRunOperator(task_id="trigger_silver", trigger_dag_id="medallion_silver")

    rg >> network >> create >> run >> delete
    [run, delete] >> trigger_silver                 # only continue if run_bronze succeeded AND the VM is gone
