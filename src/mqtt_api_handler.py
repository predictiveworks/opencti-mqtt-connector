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
import json
import random
import ssl

import paho.mqtt.client as mqtt

#
# The MQTT topics assigned to the specific data types
#
INDICATOR_CREATE_TOPIC = "opencti/indicator/create"
INDICATOR_UPDATE_TOPIC = "opencti/indicator/update"

OBSERVABLE_CREATE_TOPIC = "opencti/observable/create"
OBSERVABLE_UPDATE_TOPIC = "opencti/observable/update"

MAX_RETRIES = 3


class MqttApiHandler:
    def __init__(
        self,
        helper,
        broker_url,
        broker_port,
        client_id,
        keep_alive,
        qos,
        username,
        password,
        ca_file,
        cert_file,
        key_file,
        ssl_verify
    ):
        # OpenCTI Helper
        self.helper = helper

        # Mqtt Parameters
        self.broker_url = broker_url
        self.broker_port = broker_port

        if len(client_id) == 0:
            #
            # Generate client identifier with prefix randomly
            #
            self.client_id = f'opencti-mqtt-{random.randint(0, 1000)}'
        else:
            self.client_id = client_id

        self.keep_alive = keep_alive
        self.qos = qos

        #
        # User authentication
        #
        self.username = username
        self.password = password

        #
        # TLS support
        #
        self.ca_file = ca_file
        self.cert_file = cert_file

        self.key_file = key_file
        self.ssl_verify = ssl_verify

        #
        # Flag to indicate whether an MQTT connection
        # was established or not
        #
        self.connected = False
        #
        # Build MQTT client
        #
        self._build_mqtt_client()

    # ***********************
    #
    # MQTT SPECIFIC
    #
    # ***********************

    #
    # MQTT connection callback to indicate whether
    # connecting to the configured MQTT broker was
    # successful or not
    #
    def _on_connect(self, client, userdata, message, rc):
        """
        Called when the MQTT broker responds to the
        connection request

        :param client:
            the client instance for this callback
        :param userdata:
            the private user data as set in Client() or userdata_set()
        :param message:
            the response message sent by the MQTT broker
        :param rc:
            the connection result
        """
        # Evaluate result code (rc)
        #
        if rc == 0:
            self.connected = True
            """
            rc = 1: Incorrect protocol version
            rc = 2: Invalid client identifier
            rc = 3: Server unavailable
            rc = 4: Invalid credentials, requesting new one
            """
        else:
            self.connected = False
            self.client.disconnect()

            message = "Cannot connect to MQTT broker. Return code: {}".format(rc)
            self.helper.log_error(message)
        return

    def _on_publish(self, client, userdata, mid):
        """
        Called when a message that was to be sent using the publish() call
        has completed transmission to the broker.
        For messages with QoS levels 1 and 2, this means that the appropriate
        handshakes have completed. For QoS 0, this simply means that the
        message has left the client. The mid variable matches the mid
        variable returned from the corresponding publish() call, to allow
        outgoing messages to be tracked.

        :param client:
            the client instance for this callback
        :param userdata:
            the private user data as set in Client() or userdata_set()
        :param mid:
            matches the mid variable returned from the corresponding
            publish() call, to allow outgoing messages to be tracked.
        """
        pass

    #
    # An internal method to publish a certain
    # MQTT message to the provided topic
    #
    def _publish(self, topic, message):
        if not self.connected:
            return

        (rc, _skip) = self.client.publish(topic, message, qos=self.qos)
        if rc == 0:
            #
            # The message was successfully published
            #
            pass
        else:
            #
            # Retry until maximum of retries is reached
            # or message has been published successfully
            #
            success = False
            retry = 0

            while (not success) and retry < MAX_RETRIES:
                (rc, _skip) = self.client.publish(topic, message, qos=self.qos)
                if rc == 0:
                    success = True
                else:
                    retry += 1

            if not success:
                self.helper.log_error(
                    "Cannot publish to topic `{}`: {}".format(topic, message)
                )

        return

    #
    # An internal method to construct an MQTT client
    # and connect to the configured MQTT broker
    #
    def _build_mqtt_client(self):

        self.client = mqtt.Client(self.client_id, clean_session=True, protocol=mqtt.MQTTv31)
        #
        # Assign connection & publish callback and set
        # user name and password if configured
        #
        self.client.on_connect = self._on_connect
        self.client.on_publish = self._on_publish
        #
        # If username and password are not empty, assign
        # to MQTT client
        #
        if len(self.username) > 0 and len(self.password) > 0:
            self.client.username_pw_set(self.username, self.password)

        #
        # The CA file controls the TSL usage
        #
        if len(self.ca_file) > 0:
            if len(self.cert_file) > 0 and len(self.key_file) > 0:
                self.client.tls_set(self.ca_file,
                                    certfile=self.cert_file,
                                    keyfile=self.key_file,
                                    cert_reqs=ssl.CERT_REQUIRED)
            else:
                self.client.tls_set(self.ca_file, cert_reqs=ssl.CERT_REQUIRED)

            self.client.tls_insecure_set(self.ssl_verify)

        self.client.connect(self.broker_url, self.broker_port, self.keep_alive)
        """
        The loop() function is a built in function that will read the 
        receive and send buffers, and process any messages it finds.       
        
        The loop_start() starts a new thread, that calls the loop method 
        at regular intervals. It also handles re-connects automatically. 
        """
        self.client.loop_start()
        return

    # ***********************
    #
    # OPENCTI SPECIFIC
    #
    # ***********************

    def handle_create(self, data):
        data_type = data["type"]
        # Handle create event
        if data_type == "indicator":
            """
            
            INDICATOR
            
            """
            self._handle_create_indicator(data)
            return
        elif data_type in [
            "artifact",
            "domain-name",
            "file",
            "ipv4-addr",
            "ipv6-addr",
            "process",
            "x-opencti-hostname"
        ]:
            """
            
            OBSERVABLE
            
            """
            self._handle_create_observable(data)
            return

        return

    def handle_update(self, data):
        data_type = data["type"]
        # Handle update event
        if data_type == "indicator":
            # Handle indicator
            self._handle_update_indicator(data)
            return
        elif data_type in [
            "artifact",
            "domain-name",
            "file",
            "ipv4-addr",
            "ipv6-addr",
            "process",
            "x-opencti-hostname"
        ]:
            # Handle observable
            self._handle_update_observable(data)
            return

        return

    def handle_delete(self, data):
        data_type = data["type"]
        # Handle delete event
        if data_type == "indicator":
            # Handle indicators
            self._handle_delete_indicator(data)
            return
        elif data_type in [
            "artifact",
            "domain-name",
            "file",
            "ipv4-addr",
            "ipv6-addr",
            "process",
            "x-opencti-hostname"
        ]:
            # Handle observables
            self._handle_delete_observable(data)
            return

        return

    #
    # INDICATOR SUPPORT
    #
    def _handle_create_indicator(self, data):
        #
        # Import indicator
        #
        indicator = self._import_indicator(data)
        if indicator is not None:
            #
            # Publish indicator: Transform into serialized
            # JSON string and assign to defined topic
            #
            message = json.dumps(indicator)
            self._publish(INDICATOR_CREATE_TOPIC, message)

        return

    def _handle_update_indicator(self, data):
        #
        # Import indicator
        #
        indicator = self._import_indicator(data)
        if indicator is not None:
            #
            # Publish indicator: Transform into serialized
            # JSON string and assign to defined topic
            #
            message = json.dumps(indicator)
            self._publish(INDICATOR_UPDATE_TOPIC, message)

        return

    def _handle_delete_indicator(self, data):
        return

    #
    # This method retrieves an indicator that
    # refers to the provided interval identifier
    # from the OpenCTI knowledge base
    #
    def _import_indicator(self, data):
        opencti_id: str = data.get("x_opencti_id", None)
        if not opencti_id:
            self.helper.log_error(
                "Cannot process data without 'x_opencti_id' field"
            )
            return None
        #
        # Retrieve the indicator by id
        #
        indicator: dict = self.helper.api.indicator.read(id=opencti_id)
        if not indicator:
            return None

        # Overwrite custom OpenCTI ID
        indicator["id"] = indicator.get("standard_id")
        return indicator

    #
    # OBSERVABLE SUPPORT
    #
    def _handle_create_observable(self, data):
        #
        # Import observable
        #
        observable = self._import_observable(data)
        if observable is not None:
            #
            # Publish observable: Transform into serialized
            # JSON string and assign to defined topic
            #
            message = json.dumps(observable)
            self._publish(OBSERVABLE_CREATE_TOPIC, message)

        return

    def _handle_update_observable(self, data):
        #
        # Import observable
        #
        observable = self._import_observable(data)
        if observable is not None:
            #
            # Publish observable: Transform into serialized
            # JSON string and assign to defined topic
            #
            message = json.dumps(observable)
            self._publish(OBSERVABLE_UPDATE_TOPIC, message)

        return

    def _handle_delete_observable(self, data):
        return

    #
    # This method retrieves an observable that
    # refers to the provided interval identifier
    # from the OpenCTI knowledge base
    #
    def _import_observable(self, data):
        opencti_id: str = data.get("x_opencti_id", None)
        if not opencti_id:
            self.helper.log_error(
                "Cannot process data without 'x_opencti_id' field"
            )
            return None
        observable: dict = self.helper.api.stix_cyber_observable.read(id=opencti_id)
        if not observable:
            return None

        # Overwrite custom OpenCTI ID
        observable["id"] = observable.get("standard_id")
        return observable

