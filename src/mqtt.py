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

        #
        # Initialize Mqtt Api
        #
        self.mqtt_api_handler = MqttApiHandler(
            self.helper,
            self.broker_url
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
        self.helper.listen_stream(self._process_message())


if __name__ == "__main__":
    MqttInstance = MqttConnector()
    MqttInstance.start()
