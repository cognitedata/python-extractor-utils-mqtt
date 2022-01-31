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
import math
import threading
import time
from dataclasses import dataclass
from typing import Optional

from cognite.extractorutils.throttle import throttled_loop

from cognite.extractorutils.mqtt import MqttExtractor
from cognite.extractorutils.mqtt.types import InsertDatapoints


@dataclass
class RawDatapoint:
    id: Optional[str]
    timestamp: Optional[float]
    value: Optional[float]


event = threading.Event()

extractor = MqttExtractor(
    name="DemoMqttExtractor",
    description="Read some datapoints over MQTT",
    version="1.0.0",
    cancelation_token=event,
)


@extractor.topic(topic="mytopic", qos=1, response_type=RawDatapoint)
def subscribe_datapoints(dp: RawDatapoint) -> InsertDatapoints:
    if dp.id is None:
        return None

    return InsertDatapoints(external_id=dp.id, datapoints=[(dp.timestamp, dp.value)])


# Method to post datapoints to MQTT. It is here just so that this example is functional.
def generate_dps() -> None:
    client = extractor._create_mqtt_client("example_publisher")

    client.connect_async(extractor.config.source.host, extractor.config.source.port, extractor.config.source.keep_alive)

    client.loop_start()

    for _ in throttled_loop(0.5, event):
        raw_data = json.dumps({"id": "sine_wave", "value": math.sin(time.time() / 10), "timestamp": time.time() * 1000})
        client.publish("mytopic", raw_data, 1)

    client.loop_stop()


thread = threading.Thread(target=generate_dps, name="publish-loop")

with extractor:
    thread.start()
    extractor.run()
    thread.join()
