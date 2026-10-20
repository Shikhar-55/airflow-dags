from datetime import datetime
from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator


with DAG(
    dag_id="bash_demo",
    description="Four simple Bash tasks",
    schedule=None,                   # only runs when you click Trigger
    start_date=datetime(2026, 1, 1),
    catchup=False,
):
    # 1. Print a message
    hello = BashOperator(
        task_id="hello",
        bash_command='echo "Hello from Bash inside Airflow!"',
    )

    # 2. Show who and where the task runs
    whoami = BashOperator(
        task_id="whoami",
        bash_command="echo user=$(whoami); echo host=$(hostname)",
    )

    # 3. Write a file
    write_file = BashOperator(
        task_id="write_file",
        bash_command='echo "written at $(date)" > /tmp/demo.txt',
    )

    # 4. Read the file back
    read_file = BashOperator(
        task_id="read_file",
        bash_command="cat /tmp/demo.txt",
    )

    hello >> whoami >> write_file >> read_file