import datetime
import json

import awsiot.greengrasscoreipc
from awsiot.greengrasscoreipc.model import (
    QOS,
    PublishToIoTCoreRequest
)
import logging

LOGGER = logging.getLogger(__name__)

class SendMQTT:
    """
    Class for sending mqtt messages through greengrass to AWS IoT core.
    Parameters
    ----------

    """
    def __init__(self, timeout=10):
        self.ipc_client=awsiot.greengrasscoreipc.connect()

        if self.ipc_client:
            LOGGER.info("IPC client for mqtt initialized")
        else:
            LOGGER.info("Issue in IPC client initialization")
            raise RuntimeError('Issue in IPC client initialization')

        self.timeout=timeout

    
    def send_mqtt_msg(self,topic,inference_msg):
        LOGGER.debug("The receieved topic : {} and the payload is : {}".format(topic,inference_msg))
        request = PublishToIoTCoreRequest(topic_name=topic, qos=QOS.AT_LEAST_ONCE, payload=bytes(json.dumps(inference_msg), "utf-8"))
        operation = self.ipc_client.new_publish_to_iot_core()
        operation.activate(request)
        future = operation.get_response()
        future.result(self.timeout)