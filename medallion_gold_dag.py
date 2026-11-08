"""
medallion_gold_dag.py   (DAG 3 of 3)

No schedule. Runs only when the Silver DAG triggers it.
Create a VM, run gold.py, delete the VM. Nothing to trigger after this;
Tableau reads the CSVs that gold.py wrote.
"""

import os
import sys
from datetime import datetime, timedelta

from airflow.sdk import DAG, Variable
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.smtp.notifications.smtp import SmtpNotifier

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
    dag_id="medallion_gold",
    schedule=None,                                  # triggered by medallion_silver
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
):
    rg = PythonOperator(task_id="create_resource_group", python_callable=vm.create_resource_group)
    network = PythonOperator(task_id="create_network", python_callable=vm.create_network)
    create = PythonOperator(task_id="create_vm", python_callable=vm.create_vm)
    run = PythonOperator(task_id="run_gold", python_callable=vm.run_layer, op_args=["gold"])
    delete = PythonOperator(task_id="delete_vm", python_callable=vm.delete_vm,
                            trigger_rule="all_done")

    rg >> network >> create >> run >> delete
