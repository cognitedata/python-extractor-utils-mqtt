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

from dataclasses import dataclass
from distutils.command.clean import clean
import json
import threading
from types import TracebackType
from typing import Callable, Dict, Iterable, List, Optional, Type, Union
from cognite.extractorutils.base import Extractor
from cognite.extractorutils.configtools import BaseConfig
from cognite.extractorutils.uploader import EventUploadQueue, RawUploadQueue, TimeSeriesUploadQueue
from cognite.extractorutils.exceptions import InvalidConfigError
import dacite
from more_itertools import peekable

import paho.mqtt.client as mqtt

from cognite.extractorutils.mqtt.mqtt import Topic
from cognite.extractorutils.mqtt.types import CdfTypes, Event, InsertDatapoints, RawRow

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
class MqttConfig(BaseConfig):
    source: SourceConfig = SourceConfig()


@dataclass
class ClientUserData:
    extractor: "MqttExtractor"


class MqttExtractor(Extractor[MqttConfig]):
    def __init__(
        self,
        *,
        name: str,
        description: str,
        version: Optional[str] = None,
        base_url: Optional[str],
        cancelation_token: threading.Event = threading.Event(),
    ):
        super(MqttExtractor, self).__init__(
            name=name,
            description=description,
            version=version,
            cancelation_token=cancelation_token,
            use_default_state_store=False,
            config_class=MqttConfig,
        )
        self.base_url = base_url or ""
        self.topics: Dict[str, Topic] = {}
    
    def handle_output(self, output: CdfTypes) -> None:
        if not isinstance(output, Iterable):
            output = [output]

        peekable_output = peekable(output)
        peek = peekable_output.peek()

        if isinstance(peek, Event):
            for event in peekable_output:
                self.event_queue.add_to_upload_queue(event)
        elif isinstance(peek, RawRow):
            for raw_row in peekable_output:
                for row in raw_row.rows:
                    self.raw_queue.add_to_upload_queue(database=raw_row.db_name, table=raw_row.table_name, raw_row=row)
        elif isinstance(peek, InsertDatapoints):
            for datapoints in peekable_output:
                self.time_series_queue.add_to_upload_queue(
                    id=datapoints.id, external_id=datapoints.external_id, datapoints=datapoints.datapoints
                )
        else:
            raise ValueError(f"Unexpected type: {type(peek)}")
    
    def __enter__(self) -> "MqttExtractor":
        super(MqttExtractor, self).__enter__()
        self.event_queue = EventUploadQueue(
            self.cognite_client, max_queue_size=10_000, max_upload_interval=60, trigger_log_level="INFO"
        ).__enter__()
        self.raw_queue = RawUploadQueue(
            self.cognite_client, max_queue_size=100_000, max_upload_interval=60, trigger_log_level="INFO"
        ).__enter__()
        self.time_series_queue = TimeSeriesUploadQueue(
            self.cognite_client,
            max_queue_size=1_000_000,
            max_upload_interval=60,
            trigger_log_level="INFO",
            create_missing=True,
        ).__enter__()

        protocol = 0
        raw_protocol = self.config.source.version

        if raw_protocol == "5" or raw_protocol == "v5" or raw_protocol == "MQTTv5":
            protocol = mqtt.MQTTv5
        elif raw_protocol == "31" or raw_protocol == "v31" or raw_protocol == "MQTTv31":
            protocol = mqtt.MQTTv31
        elif raw_protocol == "311" or raw_protocol == "v311" or raw_protocol == "MQTTv311":
            protocol = mqtt.MQTTv311
        else:
            raise InvalidConfigError(f"Unknown MQTT protocol version {raw_protocol}")

        self.client = mqtt.Client(
            self.config.source.client_id,
            clean_session=True,
            protocol=protocol,
            transport=self.config.source.transport)

        self.client.user_data_set(ClientUserData(extractor=self))

        if self.config.source.username:
            self.client.username_pw_set(self.config.source.username, self.config.source.password)
            
        return self

    def __exit__(
        self, exc_type: Optional[Type[BaseException]], exc_val: Optional[BaseException], exc_tb: Optional[TracebackType]
    ) -> bool:
        self.event_queue.__exit__(exc_type, exc_val, exc_tb)
        self.raw_queue.__exit__(exc_type, exc_val, exc_tb)
        self.time_series_queue.__exit__(exc_type, exc_val, exc_tb)
        return super(MqttExtractor, self).__exit__(exc_type, exc_val, exc_tb)

    def on_message(client: mqtt.Client, userdata: ClientUserData, msg: mqtt.MQTTMessage):
        topic = userdata.extractor.topics.get(msg.topic, None)
        if topic is None:
            return

        raw_payload = json.loads(msg.payload)
        payload = dacite.from_dict(topic.response_type, raw_payload)
        result = topic.implementation(payload)

        userdata.extractor.handle_output(result)

    def run(self) -> None:
        if not self.started:
            raise ValueError("You must run the extractor in a context manager")

        for topic in self.topics:
            self.client.subscribe(topic.topic, topic.qos)

        self.client.on_message = self.on_message

        self.client.connect_async(self.config.source.host, self.config.source.port, self.config.source.keep_alive)

        self.client.loop_start()

        self.cancelation_token.wait()

        self.client.loop_stop()
