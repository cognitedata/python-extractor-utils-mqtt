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
from typing import Callable, Generic, Type, TypeVar
import paho.mqtt.client as mqtt

from cognite.extractorutils.mqtt.types import CdfTypes

ResponseType = TypeVar("ResponseType")


@dataclass
class Topic(Generic[ResponseType]):
    implementation: Callable[[ResponseType], CdfTypes]
    topic: str
    qos: int
    client: mqtt.Client
    response_type: Type[ResponseType]
