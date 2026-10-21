"""
Ephemeral VM pipeline on Azure, every 10 minutes.

    create_vm  ->  run_pipeline  ->  delete_vm

The VM only exists for the few minutes the pipeline needs.
"""
from datetime import datetime
import re
import secrets

from airflow.sdk import DAG, Variable
from airflow.providers.standard.operators.python import PythonOperator
from azure.identity import DefaultAzureCredential
from azure.mgmt.compute import ComputeManagementClient

# ---- settings --------------------------------------------------------------
RESOURCE_GROUP = "rg-pipeline"
LOCATION = "westus"
VM_NAME = "pipeline-vm"
VM_SIZE = "Standard_B2pts_v2"          # cheap ARM machine; change this and the image follows
NIC_NAME = "pipeline-vm-nic"           # created once by hand

# ARM sizes have a "p" after the number (B2pts, D2ps, E2pds...) and need the arm64 image.
if re.match(r"^Standard_[A-Z]+[0-9]+[a-z]*p", VM_SIZE):
    IMAGE_SKU = "server-arm64"
else:
    IMAGE_SKU = "server"

IMAGE = {"publisher": "Canonical", "offer": "ubuntu-24_04-lts", "sku": IMAGE_SKU, "version": "latest"}

REPO_URL = "https://github.com/Shikhar-55/ephemeral_data_pipeline.git"
REPO_SUBDIR = "test_pipeline"          # folder inside the repo that has main.py


def get_azure():
    """Connect to Azure using the Airflow VM's identity (no passwords)."""
    sub = Variable.get("AZURE_SUBSCRIPTION_ID")
    return ComputeManagementClient(DefaultAzureCredential(), sub), sub


# ---- task 1: create the VM ---------------------------------------------------
def create_vm():
    azure, sub = get_azure()
    nic = f"/subscriptions/{sub}/resourceGroups/{RESOURCE_GROUP}/providers/Microsoft.Network/networkInterfaces/{NIC_NAME}"

    vm = {
        "location": LOCATION,
        "properties": {
            "hardwareProfile": {"vmSize": VM_SIZE},
            "storageProfile": {
                "imageReference": IMAGE,
                "osDisk": {"createOption": "FromImage", "deleteOption": "Delete"},
            },
            "osProfile": {"computerName": VM_NAME, "adminUsername": "azureuser",
                          "adminPassword": secrets.token_urlsafe(24) + "Aa1!"},
            "networkProfile": {"networkInterfaces": [{"id": nic}]},
        },
    }
    print(f"Creating {VM_SIZE} in {LOCATION} with image {IMAGE_SKU}... (1 to 3 minutes)")
    azure.virtual_machines.begin_create_or_update(RESOURCE_GROUP, VM_NAME, vm).result()
    print("VM is running")


# ---- task 2: clone the repo and run main.py on the VM --------------------------
def run_pipeline():
    azure, _ = get_azure()
    supabase_url = Variable.get("supabase_url")
    supabase_key = Variable.get("supabase_service_key")

    script = [
        "set -e",                                                   # stop on first error
        "apt-get update -qq",
        "apt-get install -y -qq git python3 python3-pip > /dev/null",
        f"git clone --depth 1 {REPO_URL} /tmp/repo",
        f"cd /tmp/repo/{REPO_SUBDIR}",
        "if [ -f requirements.txt ]; then pip3 install -q --break-system-packages -r requirements.txt; fi",
        f"export SUPABASE_URL='{supabase_url}'",
        f"export SUPABASE_SERVICE_KEY='{supabase_key}'",
        "python3 main.py && echo PIPELINE_OK",                      # marker so Airflow knows it worked
    ]

    print("Running pipeline on the VM...")
    result = azure.virtual_machines.begin_run_command(RESOURCE_GROUP, VM_NAME, {
        "commandId": "RunShellScript",
        "script": script,
    }).result()

    output = result.value[0].message
    print(output)
    if "PIPELINE_OK" not in output:
        raise RuntimeError("Pipeline failed on the VM, see output above")


# ---- task 3: delete the VM ---------------------------------------------------
def delete_vm():
    azure, _ = get_azure()
    print("Deleting VM and its disk...")
    azure.virtual_machines.begin_delete(RESOURCE_GROUP, VM_NAME).result()
    print("VM deleted")


# ---- the DAG -----------------------------------------------------------------
with DAG(
    dag_id="ephemeral_pipeline",
    schedule="*/10 * * * *",       # every 10 minutes
    start_date=datetime(2026, 1, 1),
    catchup=False,                 # don't backfill missed runs
    max_active_runs=1,             # never two VMs at once
):
    create = PythonOperator(task_id="create_vm", python_callable=create_vm)
    run = PythonOperator(task_id="run_pipeline", python_callable=run_pipeline)
    delete = PythonOperator(task_id="delete_vm", python_callable=delete_vm,
                            trigger_rule="all_done")   # always clean up, even on failure

    create >> run >> delete