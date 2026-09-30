from datetime import datetime
import secrets

from airflow.sdk import DAG, Variable
from airflow.providers.standard.operators.python import PythonOperator
from azure.identity import DefaultAzureCredential
from azure.mgmt.compute import ComputeManagementClient

RESOURCE_GROUP = "rg-pipeline"
VM_NAME = "pipeline-vm"


def get_azure():
    """Connect to Azure using the Airflow VM's identity (no passwords)."""
    sub = Variable.get("AZURE_SUBSCRIPTION_ID")
    return ComputeManagementClient(DefaultAzureCredential(), sub), sub


def create_vm():
    azure, sub = get_azure()
    nic = f"/subscriptions/{sub}/resourceGroups/{RESOURCE_GROUP}/providers/Microsoft.Network/networkInterfaces/pipeline-vm-nic"

    vm = {
        "location": "canadacentral",
        "properties": {
            "hardwareProfile": {"vmSize": "Standard_B2pts_v2"},
            "storageProfile": {
                "imageReference": {"publisher": "Canonical", "offer": "ubuntu-24_04-lts",
                                   "sku": "server-arm64", "version": "latest"},
                "osDisk": {"createOption": "FromImage", "deleteOption": "Delete"},
            },
            "osProfile": {"computerName": VM_NAME, "adminUsername": "azureuser",
                          "adminPassword": secrets.token_urlsafe(24) + "Aa1!"},
            "networkProfile": {"networkInterfaces": [{"id": nic}]},
        },
    }
    print("Creating VM... (1 to 3 minutes)")
    azure.virtual_machines.begin_create_or_update(RESOURCE_GROUP, VM_NAME, vm).result()
    print("VM is running")


def run_hello():
    azure, _ = get_azure()
    result = azure.virtual_machines.begin_run_command(RESOURCE_GROUP, VM_NAME, {
        "commandId": "RunShellScript",
        "script": ["echo Hello from the new VM!", "uname -m", "nproc"],
    }).result()
    print(result.value[0].message)


def delete_vm():
    azure, _ = get_azure()
    print("Deleting VM and its disk...")
    azure.virtual_machines.begin_delete(RESOURCE_GROUP, VM_NAME).result()
    print("VM deleted")


with DAG(
    dag_id="ephemeral_vm_demo",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
):
    create = PythonOperator(task_id="create_vm", python_callable=create_vm)
    hello = PythonOperator(task_id="run_hello", python_callable=run_hello)
    delete = PythonOperator(task_id="delete_vm", python_callable=delete_vm,
                            trigger_rule="all_done")  # always clean up

    create >> hello >> delete