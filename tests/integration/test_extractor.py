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
import random
import threading
import time
import unittest
from dataclasses import dataclass
from typing import Optional

from cognite.extractorutils.throttle import throttled_loop
from cognite.extractorutils.uploader_types import InsertDatapoints

from cognite.extractorutils.mqtt.extractor import MqttExtractor


@dataclass
class RawDatapoint:
    id: Optional[str]
    timestamp: Optional[float]
    value: Optional[float]


test_id = random.randint(0, 2**31)


class IntegrationTests(unittest.TestCase):
    ts_1: str = f"mqtt_util_integration_ts_test_1-{test_id}"

    def setUp(self) -> None:
        self.event = threading.Event()
        self.extractor = MqttExtractor(
            name="test_mqtt_extractor",
            description="Test mqtt extractor",
            version="1.0.0",
            cancelation_token=self.event,
            override_path="config_test.yml",
        )
        self.extractor._upload_interval = 1

    def tearDown(self) -> None:
        self.extractor.cognite_client.time_series.delete(external_id=self.ts_1, ignore_unknown_ids=True)
        self.mqttClient.loop_stop()

    def test_ts_extraction(self) -> None:
        @self.extractor.topic(topic="ts-topic-1", qos=1, response_type=RawDatapoint)
        def handle_ts(dp: RawDatapoint) -> None:
            return InsertDatapoints(external_id=dp.id, datapoints=[(dp.timestamp, dp.value)])

        def run_loop() -> None:
            iter = 0
            self.mqttClient = self.extractor._create_mqtt_client("integration-test-publisher")
            self.mqttClient.connect_async(
                self.extractor.config.source.host,
                self.extractor.config.source.port,
                self.extractor.config.source.keep_alive,
            )
            self.mqttClient.loop_start()
            for _ in throttled_loop(1, self.event):
                raw_data = json.dumps(
                    {"id": self.ts_1, "value": math.sin(time.time() / 10), "timestamp": time.time() * 1000}
                )
                self.mqttClient.publish("ts-topic-1", raw_data, 1)

                try:
                    points = self.extractor.cognite_client.datapoints.retrieve(
                        external_id=self.ts_1, start="1w-ago", end="now", limit=None, ignore_unknown_ids=True
                    )
                    if points is not None and points.count > 1:
                        self.event.set()
                    else:
                        iter += 1
                        if iter > 5:
                            self.event.set()
                except:
                    self.event.set()
            self.mqttClient.loop_stop()

        thread = threading.Thread(target=run_loop, name="publish-loop")

        with self.extractor:
            thread.start()
            self.extractor.run()
            thread.join()

        points = self.extractor.cognite_client.datapoints.retrieve(
            external_id=self.ts_1, start="1w-ago", end="now", limit=None, ignore_unknown_ids=True
        )

        assert points is not None and len(points.value) > 0
