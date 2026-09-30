from datetime import datetime
import secrets

from airflow.sdk import DAG, Variable
from airflow.providers.standard.operators.python import PythonOperator
from azure.identity import DefaultAzureCredential
from azure.mgmt.compute import ComputeManagementClient

RESOURCE_GROUP = "rg-pipeline"
VM_NAME = "pipeline-vm"


def get_azure():
    """Connect to Azure as the Airflow VM's identity (no passwords needed)."""
    subscription_id = Variable.get("AZURE_SUBSCRIPTION_ID")
    return ComputeManagementClient(DefaultAzureCredential(), subscription_id), subscription_id


def create_vm():
    azure, sub = get_azure()
    subnet = f"/subscriptions/{sub}/resourceGroups/{RESOURCE_GROUP}/providers/Microsoft.Network/virtualNetworks/pipeline-vnet/subnets/default"

    vm = {
        "location": "canadacentral",
        "hardware_profile": {"vm_size": "Standard_B2pts_v2"},  # small ARM machine
        "storage_profile": {
            "image_reference": {"publisher": "Canonical", "offer": "ubuntu-24_04-lts",
                                "sku": "server-arm64", "version": "latest"},
            "os_disk": {"create_option": "FromImage", "delete_option": "Delete"},
        },
        "os_profile": {"computer_name": VM_NAME, "admin_username": "azureuser",
                       "admin_password": secrets.token_urlsafe(24) + "Aa1!"},
        "network_profile": {
            "network_api_version": "2020-11-01",
            "network_interface_configurations": [{
                "name": f"{VM_NAME}-nic", "primary": True, "delete_option": "Delete",
                "ip_configurations": [{
                    "name": "ipconfig1", "subnet": {"id": subnet},
                    "public_ip_address_configuration": {
                        "name": f"{VM_NAME}-ip", "sku": {"name": "Standard"},
                        "public_ip_allocation_method": "Static", "delete_option": "Delete"},
                }],
            }],
        },
    }
    print("Creating VM... (1 to 3 minutes)")
    azure.virtual_machines.begin_create_or_update(RESOURCE_GROUP, VM_NAME, vm).result()
    print("VM is running")


def run_hello():
    azure, _ = get_azure()
    result = azure.virtual_machines.begin_run_command(RESOURCE_GROUP, VM_NAME, {
        "command_id": "RunShellScript",
        "script": ["echo Hello from the new VM!", "uname -m", "nproc"],
    }).result()
    print(result.value[0].message)


def delete_vm():
    azure, _ = get_azure()
    print("Deleting VM (disk, network card and IP are deleted with it)...")
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