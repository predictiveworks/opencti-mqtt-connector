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
from stix2 import Indicator


class MqttApiHandler:
    def __init__(
        self,
        helper,
        broker_url
    ):
        # OpenCTI Helper
        self.helper = helper

        # Mqtt Parameters
        self.broker_url = broker_url

    def handle_create(self, data):
        data_type = data["type"]
        # Handle create event
        if data_type == "indicator":
            # Handle indicator
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
            # Handle observable
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
        return

    def _handle_update_indicator(self, data):
        #
        # Import indicator as STIX 2 bundle
        #
        indicator = self._import_indicator(data)
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
        return Indicator(**indicator, allow_custom=True)

    #
    # OBSERVABLE SUPPORT
    #
    def _handle_create_observable(self, entity):
        return

    def _handle_update_observable(self, data):
        return

    def _handle_delete_observable(self, data):
        return

    #
    # This method retrieves an observable that
    # refers to the provided interval identifier
    # from the OpenCTI knowledge base
    #
    def _import_observable(self, data):
        return

