"""
medallion_utils.py
Shared by medallion_bronze_dag.py, medallion_silver_dag.py and medallion_gold_dag.py.

Azure VM functions are the same ones used in ephemeral_pipeline_dag.py.
The only new piece is run_layer(), which clones the pipeline repo on the VM
and runs one layer script (bronze.py, silver.py or gold.py).
"""

import logging
import secrets

from airflow.sdk import Variable
from azure.identity import DefaultAzureCredential
from azure.mgmt.compute import ComputeManagementClient
from azure.mgmt.network import NetworkManagementClient
from azure.mgmt.resource.resources import ResourceManagementClient

RESOURCE_GROUP = "rg-pipeline"
LOCATION = "westus"
VM_NAME = "pipeline-vm"
VM_SIZE = "Standard_B2pts_v2"

REPO_URL = "https://github.com/Shikhar-55/earthquake_medallion_pipeline.git"
REPO_SUBDIR = "pipeline"               # folder inside the repo that has bronze.py etc.

logging.getLogger("azure").setLevel(logging.WARNING)   # hide the Azure SDK's HTTP chatter in task logs


def get_azure():
    """Connect to Azure using the Airflow VM's identity (no passwords)."""
    sub = Variable.get("AZURE_SUBSCRIPTION_ID")
    return ComputeManagementClient(DefaultAzureCredential(), sub), sub


def create_resource_group():
    """Create the resource group if it does not exist. Does nothing if it does."""
    sub = Variable.get("AZURE_SUBSCRIPTION_ID")
    resources = ResourceManagementClient(DefaultAzureCredential(), sub)
    if resources.resource_groups.check_existence(RESOURCE_GROUP):
        print(f"Resource group {RESOURCE_GROUP} already exists")
    else:
        resources.resource_groups.create_or_update(RESOURCE_GROUP, {"location": LOCATION})
        print(f"Created resource group {RESOURCE_GROUP} in {LOCATION}")


def create_network():
    """Creates VNet, public IP and NIC on the first run, finds them on later runs."""
    sub = Variable.get("AZURE_SUBSCRIPTION_ID")
    network = NetworkManagementClient(DefaultAzureCredential(), sub)
    base = f"/subscriptions/{sub}/resourceGroups/{RESOURCE_GROUP}/providers/Microsoft.Network"

    network.virtual_networks.begin_create_or_update(RESOURCE_GROUP, "pipeline-vnet", {
        "location": LOCATION,
        "properties": {
            "addressSpace": {"addressPrefixes": ["10.0.0.0/16"]},
            "subnets": [{"name": "default", "properties": {"addressPrefix": "10.0.0.0/24"}}],
        },
    }).result()
    print("VNet ready")

    network.public_ip_addresses.begin_create_or_update(RESOURCE_GROUP, "pipeline-vm-pip", {
        "location": LOCATION,
        "sku": {"name": "Standard"},
        "properties": {"publicIPAllocationMethod": "Static"},
    }).result()
    print("Public IP ready")

    network.network_interfaces.begin_create_or_update(RESOURCE_GROUP, "pipeline-vm-nic", {
        "location": LOCATION,
        "properties": {
            "ipConfigurations": [{
                "name": "ipconfig1",
                "properties": {
                    "subnet": {"id": f"{base}/virtualNetworks/pipeline-vnet/subnets/default"},
                    "publicIPAddress": {"id": f"{base}/publicIPAddresses/pipeline-vm-pip"},
                },
            }],
        },
    }).result()
    print("NIC ready")


def create_vm():
    azure, sub = get_azure()
    nic = f"/subscriptions/{sub}/resourceGroups/{RESOURCE_GROUP}/providers/Microsoft.Network/networkInterfaces/pipeline-vm-nic"

    vm = {
        "location": LOCATION,
        "properties": {
            "hardwareProfile": {"vmSize": VM_SIZE},
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


def run_layer(layer):
    """Clone the pipeline repo on the VM and run <layer>.py.
    The storage secrets come from Airflow Variables and are passed to the
    script as environment variables, so nothing secret is ever on GitHub."""
    azure, _ = get_azure()
    storage_account = Variable.get("STORAGE_ACCOUNT")
    storage_key = Variable.get("STORAGE_SECRET_KEY")   # "SECRET" in the name makes Airflow mask it in the UI

    script = [
        "set -e",                                                   # stop on first error
        "export DEBIAN_FRONTEND=noninteractive",
        "apt-get update -qq",
        "apt-get install -y -qq git python3 python3-pip > /dev/null",
        "rm -rf /tmp/repo",                                         # a retry reuses the VM, so clear the old clone
        f"git clone --depth 1 {REPO_URL} /tmp/repo",
        "cd /tmp/repo",
        "pip3 install -q --break-system-packages -r requirements.txt",
        f"cd {REPO_SUBDIR}",
        f"export STORAGE_ACCOUNT='{storage_account}'",
        f"export STORAGE_KEY='{storage_key}'",
        f"python3 {layer}.py && echo PIPELINE_OK",                 # marker so Airflow knows it worked
    ]

    print(f"Running {layer}.py on the VM...")
    result = azure.virtual_machines.begin_run_command(RESOURCE_GROUP, VM_NAME, {
        "commandId": "RunShellScript",
        "script": script,
    }).result()

    output = result.value[0].message
    print(output)                                                   # the layer's own log lines show up here
    if "PIPELINE_OK" not in output:
        raise RuntimeError(f"{layer}.py failed on the VM, see output above")


def delete_vm():
    azure, _ = get_azure()
    print("Deleting VM and its disk...")
    azure.virtual_machines.begin_delete(RESOURCE_GROUP, VM_NAME).result()
    print("VM deleted")
