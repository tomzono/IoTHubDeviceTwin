import os
from azure.iot.device import ProvisioningDeviceClient
from azure.iot.device import IoTHubDeviceClient
from azure.iot.device import Message
import DPSconfig
provisioning_host = DPSconfig.provisioning_group_host
id_scope = DPSconfig.group_id_scope

def register_device(registration_id,derived_device_key):
    provisioning_device_client = ProvisioningDeviceClient.create_from_symmetric_key(
        provisioning_host=provisioning_host,
        registration_id=registration_id,
        id_scope=id_scope,
        symmetric_key=derived_device_key,
    )
    return provisioning_device_client.register()
