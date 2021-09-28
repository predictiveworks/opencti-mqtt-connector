#
# Copyright (c) 2020 - 2021 Dr. Krusche & Partner PartG. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.
#
# @author Stefan Krusche, Dr. Krusche & Partner PartG
#
#

import os
import yaml
import json

from pycti import OpenCTIConnectorHelper, get_config_variable
from mqtt_api_handler import MqttApiHandler


class MqttConnector:
    def __init__(self):
        # Initialize parameters and OpenCTI helper
        config_file_path = os.path.dirname(os.path.abspath(__file__)) + "/config.yml"
        config = (
            yaml.load(open(config_file_path), Loader=yaml.FullLoader)
            if os.path.isfile(config_file_path)
            else {}
        )
        self.helper = OpenCTIConnectorHelper(config)
        #
        # Initialize Mqtt
        #
        self.broker_url = get_config_variable("MQTT_BROKER_URL", ["mqtt", "broker_url"], config)
        self.broker_port = get_config_variable("MQTT_BROKER_PORT", ["mqtt", "broker_port"], config)

        self.username = get_config_variable("MQTT_USERNAME", ["mqtt", "username"], config)
        self.password = get_config_variable("MQTT_PASSWORD", ["mqtt", "password"], config)

        self.client_id = get_config_variable("MQTT_CLIENT_ID", ["mqtt", "client_id"], config)
        self.keep_alive = get_config_variable("MQTT_KEEP_ALIVE", ["mqtt", "keep_alive"], config)

        self.qos = get_config_variable("MQTT_QOS", ["mqtt", "qos"], config)
        #
        # TLS SUPPORT
        #
        self.ca_file = get_config_variable("MQTT_CA_FILE", ["mqtt", "ca_file"], config)
        self.cert_file = get_config_variable("MQTT_CERT_FILE", ["mqtt", "cert_file"], config)
        self.key_file = get_config_variable("MQTT_KEY_FILE", ["mqtt", "key_file"], config)
        self.ssl_verify = get_config_variable("MQTT_SSL_VERIFY", ["mqtt", "ssl_verify"], config)

        #
        # Initialize Mqtt Api
        #
        self.mqtt_api_handler = MqttApiHandler(
            self.helper,
            # the hostname or IP address of the remote broker
            self.broker_url,
            # the network port of the server host to connect to
            self.broker_port,
            self.client_id,
            # maximum period in seconds allowed between communications with the broker
            self.keep_alive,
            self.qos,
            # username for broker authentication
            self.username,
            # password for broker authentication
            self.password,
            # a string path to the Certificate Authority certificate files that are to
            # be treated as trusted by this client
            self.ca_file,
            # string path pointing to the PEM encoded client certificate file
            self.cert_file,
            # string path pointing to the PEM encoded private key file
            self.key_file,
            # if true, verification of the server hostname in the server certificate
            self.ssl_verify
        )

    def _process_message(self, msg):
        try:
            data = json.loads(msg.data)["data"]
        except ValueError:
            raise ValueError("Cannot process message: " + msg)

        # Handle create
        if msg.event == "create":
            self.helper.log_info(
                "[CREATE] Processing data {" + data["x_opencti_id"] + "}"
            )
            self.mqtt_api_handler.handle_create(data)
            return
        # Handle update
        elif msg.event == "update":
            self.helper.log_info(
                "[UPDATE] Processing data {" + data["x_opencti_id"] + "}"
            )
            self.mqtt_api_handler.handle_update(data)
            return
        # Handle delete
        elif msg.event == "delete":
            self.helper.log_info(
                "[DELETE] Processing data {" + data["x_opencti_id"] + "}"
            )
            self.mqtt_api_handler.handle_delete(data)
            return

        return None

    def start(self):
        self.helper.listen_stream(self._process_message)


if __name__ == "__main__":
    MqttInstance = MqttConnector()
    MqttInstance.start()
