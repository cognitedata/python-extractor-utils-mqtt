# Cognite `extractor-utils` MQTT extension

The MQTT extension for [Cognite `extractor-utils`](https://github.com/cognitedata/python-extractor-utils) provides a way
to easily write your own extractors for systems exposing an MQTT interface.

The library is currently under development, and should not be used in production environments yet.


## Overview

The MQTT extension for extractor utils subscribes to MQTT topics, automatically serializes the payload into user-defined
DTO classes, and handles uploading of data to CDF.

The only part of the extractor necessary to for a user to implement are

 * Describing the payload schema using Python `dataclass`es
 * Implementing a mapping from the source data model to the CDF data model

As an example, consider this example payload:

``` json
{
    "elements": [
        {
            "pumpId": "bridge-pump-1453",
            "startTime": "2022-02-27T12:13:00",
            "duration": 16,
        },
        {
            "pumpId": "bridge-pump-254",
            "startTime": "2022-02-26T16:12:23",
            "duration": 124,
        },
    ]
}
```

We want to make an extractor that can turn these MQTT messages into CDF events. First, we need to create some data
classes describing the expected schema of the payloads:

```python
@dataclass
class PumpEvent:
    pumpId: str
    startTime: str
    duration: int

@dataclass
class PumpEventList:
    elements: List[PumpEvent]
```

Then, we can create an `MqttExtractor` instance, subscribe to the appropriate topic, and convert the payload into CDF
events:

```python
extractor = MqttExtractor(
    name="PumpMqttExtractor",
    description="Extracting pumping events from an MQTT source",
    version="1.0.0",
)

@extractor.topic(topic="mytopic", qos=1, response_type=PumpEventList)
def subscribe_pump_events(events: PumpEventList) -> Iterable[Event]:
    external_id_prefix = MqttExtractor.get_current_config_file()

    for pump_event in events.elements:
        start_time = arrow.get(pump_event.startTime)
        end_time = start_time.shift(seconds=pump_event.duration)

        yield Event(
            external_id=f"{external_id_prefix}{pump_event.pumpId}-{uuid.uuid4()}",
            start_time=start_time.int_timestamp*1000,
            end_time=end_time.int_timestamp*1000,
        )

with extractor:
    extractor.run()
```

A demo example is provided in the [`example.py`](./example.py) file.


## Contributing

See the [contribution guide for `extractor-utils`](https://github.com/cognitedata/python-extractor-utils#contributing)
for details on contributing.

