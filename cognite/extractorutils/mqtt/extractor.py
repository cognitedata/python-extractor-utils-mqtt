#  Copyright 2022 Cognite AS
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import json
import threading
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional, Tuple, Type

import dacite
import paho.mqtt.client as mqtt
from cognite.extractorutils.exceptions import InvalidConfigError
from cognite.extractorutils.uploader_extractor import UploaderExtractor, UploaderExtractorConfig
from cognite.extractorutils.uploader_types import CdfTypes

from cognite.extractorutils.mqtt.mqtt import ResponseType, Topic


@dataclass
class SourceConfig:
    username: Optional[str]
    password: Optional[str]
    client_id: str
    host: str
    port: int = 1883
    keep_alive: int = 60
    version: str = "5"
    transport: str = "tcp"


@dataclass
class MqttConfig(UploaderExtractorConfig):
    source: SourceConfig


@dataclass
class ClientUserData:
    extractor: "MqttExtractor"


def on_message(client: mqtt.Client, userdata: ClientUserData, msg: mqtt.MQTTMessage) -> None:
    userdata.extractor.logger.debug("Received %s bytes on topic: %s", len(msg.payload), msg.topic)
    topic = userdata.extractor.topics.get(msg.topic, None)
    if topic is None:
        return

    raw_payload = json.loads(msg.payload)
    payload = dacite.from_dict(topic.response_type, raw_payload)
    result = topic.implementation(payload)

    userdata.extractor.handle_output(result)


def on_connect(
    client: mqtt.Client, userdata: ClientUserData, flags: Dict[str, str], rc: int, properties: None = None
) -> None:
    ext = userdata.extractor
    if rc == 0:
        ext.logger.info("Successfully connected to MQTT broker")
        connects: List[Tuple[str, int]] = []
        for topic in ext.topics.values():
            ext.logger.info("Subscribing to messages on topic %s", topic.topic)
            connects.append((topic.topic, topic.qos))
        client.subscribe(connects)
    elif rc == 1:
        ext.logger.error("Failed to connect to MQTT broker: Connection refused - incorrect protocol version")
    elif rc == 2:
        ext.logger.error("Failed to connect to MQTT broker: Connection refused - invalid client identifier")
    elif rc == 3:
        ext.logger.error("Failed to connect to MQTT broker: Connection refused - server unavailable")
    elif rc == 4:
        ext.logger.error("Failed to connect to MQTT broker: Connection refused - bad username or password")
    elif rc == 5:
        ext.logger.error("Failed to connect to MQTT broker: Connection refused - not authorised")
    else:
        ext.logger.error("Invalid response code from connect")


class MqttExtractor(UploaderExtractor[MqttConfig]):
    def __init__(
        self,
        *,
        name: str,
        description: str,
        version: Optional[str] = None,
        cancelation_token: threading.Event = threading.Event(),
        override_path: Optional[str] = None,
    ):
        super(MqttExtractor, self).__init__(
            name=name,
            description=description,
            version=version,
            cancelation_token=cancelation_token,
            use_default_state_store=False,
            config_class=MqttConfig,
            config_file_path=override_path,
        )
        self.topics: Dict[str, Topic] = {}
        self._upload_interval = 60

    def topic(
        self,
        *,
        topic: str,
        qos: int,
        response_type: Type[ResponseType],
    ) -> Callable[[Callable[[ResponseType], CdfTypes]], Callable[[ResponseType], CdfTypes]]:
        def decorator(func: Callable[[ResponseType], CdfTypes]) -> Callable[[ResponseType], CdfTypes]:
            self.topics[topic] = Topic(implementation=func, topic=topic, qos=qos, response_type=response_type)
            return func

        return decorator

    def _create_mqtt_client(self, client_id: str) -> mqtt.Client:
        protocol = 0
        raw_protocol = self.config.source.version

        if raw_protocol == "5" or raw_protocol == "v5" or raw_protocol == "MQTTv5":
            protocol = mqtt.MQTTv5
        elif raw_protocol == "31" or raw_protocol == "v31" or raw_protocol == "MQTTv31":
            protocol = mqtt.MQTTv31
        elif raw_protocol == "311" or raw_protocol == "v311" or raw_protocol == "MQTTv311":
            protocol = mqtt.MQTTv311
        else:
            raise InvalidConfigError(f"Unknown MQTT protocol version {raw_protocol}, legal values are 5, 31, or 311")

        client = mqtt.Client(client_id, protocol=protocol, transport=self.config.source.transport)

        client.user_data_set(ClientUserData(extractor=self))

        if self.config.source.username:
            client.username_pw_set(self.config.source.username, self.config.source.password)

        return client

    def __enter__(self) -> "MqttExtractor":
        super(MqttExtractor, self).__enter__()

        self.client: mqtt.Client = self._create_mqtt_client(self.config.source.client_id)

        return self

    def run(self) -> None:
        if not self.started:
            raise ValueError("You must run the extractor in a context manager")

        self.client.on_message = on_message
        self.client.on_connect = on_connect

        self.client.connect_async(self.config.source.host, self.config.source.port, self.config.source.keep_alive)

        self.client.loop_start()

        self.cancelation_token.wait()

        self.client.loop_stop()
