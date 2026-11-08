"""
medallion_silver_dag.py   (DAG 2 of 3)

No schedule. Runs only when the Bronze DAG triggers it.
Create a VM, run silver.py, delete the VM, then trigger the Gold DAG.
"""

import os
import sys
from datetime import datetime, timedelta

from airflow.sdk import DAG, Variable
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.smtp.notifications.smtp import SmtpNotifier
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

# The DAGs are synced into a subfolder of the dags folder, so add this file's own
# folder to the import path before importing the shared helper next to it
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import medallion_utils as vm

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    # Email when a task has used up all its retries (uses the smtp_default connection)
    "on_failure_callback": SmtpNotifier(to=Variable.get("ALERT_EMAIL")),
}

with DAG(
    dag_id="medallion_silver",
    schedule=None,                                  # triggered by medallion_bronze
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
):
    rg = PythonOperator(task_id="create_resource_group", python_callable=vm.create_resource_group)
    network = PythonOperator(task_id="create_network", python_callable=vm.create_network)
    create = PythonOperator(task_id="create_vm", python_callable=vm.create_vm)
    run = PythonOperator(task_id="run_silver", python_callable=vm.run_layer, op_args=["silver"])
    delete = PythonOperator(task_id="delete_vm", python_callable=vm.delete_vm,
                            trigger_rule="all_done")
    trigger_gold = TriggerDagRunOperator(task_id="trigger_gold", trigger_dag_id="medallion_gold")

    rg >> network >> create >> run >> delete
    [run, delete] >> trigger_gold
