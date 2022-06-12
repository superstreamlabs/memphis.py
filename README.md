![](https://memphis-public-files.s3.eu-central-1.amazonaws.com/Vector_page-0001.jpg)
<br><br>
![Github tag](https://img.shields.io/github/v/release/memphisdev/memphis.js) [![Maintenance](https://img.shields.io/badge/Maintained%3F-yes-green.svg)](https://github.com/Memphisdev/memphis-broker/commit-activity) [![GoReportCard example](https://goreportcard.com/badge/github.com/nanomsg/mangos)](https://goreportcard.com/report/github.com/nanomsg/mangos)

Too many data sources and too many schemas? Looking for a messaging queue to scale your data-driven architecture? Require greater performance for your data streams? Your architecture is based on post-processing data, and you want to switch to real-time in minutes instead of months? Struggle to install, configure and update Kafka/RabbitMQ/and other MQs?

**Meet Memphis**

**[Memphis](https://memphis.dev)** is a dev-first, cloud-native, event processing platform made out of devs' struggles with tools like Kafka, RabbitMQ, NATS, and others, allowing you to achieve all other message brokers' benefits in a fraction of the time.<br><br>
**[Memphis](https://memphis.dev) delivers:**

- The most simple to use Message Broker (With the same behaivour as NATS and Kafka)
- State-of-the-art UI and CLI
- No need for Kafka Connect, Kafka Streams, ksql. All the tools you need are under the same roof
- An in-line data processing in any programming language
- Out-of-the-box deep observability of every component

RabbitMQ has Queues, Kafka as Topics, **Memphis has Stations.**

#### TL;DR

**On Day 1 (For the DevOps heros out there) -**<br>
Memphis platform provides the same old and loved behavior (Produce-Consume) of other data lakes and MQs, but removes completly the complexity barriers, messy documentation, ops, manual scale, orchestration and more.

**On Day 2 (For the Developers) -**
Developer lives with developing real-time, event-driven apps that are too complex.
Consumers and Producers are filled with logic, data orchestration is needed between the different services, no GUI to understand metrics and flows, lack of monitoring, hard to implement SDKs, etc.

No More.

In the coming versions, Memphis will answer the challenges above,<br>and recude 90% of dev work arround building a real-time / event-driven / data-driven apps.

---

**Purpose of this repo**<br>
For Memphis node.py SDK

**Table of Contents**

- [Current SDKs](#current-sdks)
- [Installation](#installation)
- [Importing](#importing)
  - [Connecting to Memphis](#connecting-to-memphis)
  - [Disconnecting from Memphis](#disconnecting-from-memphis)
  - [Creating a Factory](#creating-a-factory)
  - [Destroying a Factory](#destroying-a-factory)
  - [Creating a Station](#creating-a-station)
  - [Retention types](#retention-types)
  - [Storage types](#storage-types)
  - [Destroying a Station](#destroying-a-station)
  - [Produce and Consume messages](#produce-and-consume-messages)
  - [Creating a Producer](#creating-a-producer)
  - [Producing a message](#producing-a-message)
  - [Destroying a Producer](#destroying-a-producer)
  - [Creating a Consumer](#creating-a-consumer)
  - [Processing messages](#processing-messages)
  - [Acknowledge a message](#acknowledge-a-message)
  - [Catching async errors](#catching-async-errors)
  - [Destroying a Consumer](#destroying-a-consumer)
- [Memphis Contributors](#memphis-contributors)
- [Contribution guidelines](#contribution-guidelines)
- [Documentation](#documentation)
- [Contact](#contact)

## Current SDKs

- [memphis-js](https://github.com/Memphisdev/memphis.js "Node.js")
- [memphis-py](https://github.com/Memphisdev/memphis.py "Python")

## Installation

First install [Memphis](https://memphis.dev) Then:

```sh
$ pip install memphis-dev
```

## Importing

```python
from memphis import Memphis
```

### Connecting to Memphis

First, we need to create Memphis object and then connect with Memphis by using `memphis.connect`.

````python
async def main():
    try:
        memphis = Memphis()
        await memphis.connect(
            host="<memphis-host>",
            username="<application type username>", # user of type root
            connection_token="<broker-token>", # broker token.
            management_port="<management-port>", # defaults to 5555
            tcp_port="<tcp-port>", # defaults to 6666
            data_port="<data-port>", # defaults to 7766
            reconnect=True, # defaults to False
            max_reconnect=10, # defaults to 10
            reconnect_interval_ms=1500, # defaults to 1500
            timeout_ms=1500 # defaults to 1500
            )
        ...
        await memphis.close()
    except Exception as e:
        print(e)
        await memphis.close()

if __name__ == '__main__':
    asyncio.run(main())

Once connected, the entire functionalities offered by Memphis are available.

### Disconnecting from Memphis

To disconnect from Memphis, call `close()` on the memphis object.

```python
await memphis.close();
````

### Creating a Factory

```python
factory = await memphis.factory(name="<factory-name>", description="")
```

### Destroying a Factory

Destroying a factory will remove all its resources (stations/producers/consumers)

```python
factory.destroy()
```

### Creating a Station

```python
station = await memphis.station(
  name="<station-name>",
  factory_name="<factory-name>",
  retention_type="message_age_sec", #message_age_sec/messages/bytes. Defaults to "message_age_sec".
  retention_value=604800, # defaults to 604800
  storage_type="file", # file/memory. Defaults to "file".
  replicas=1, # defaults to 1
  dedup_enabled=False, # defaults to false
  dedup_window_ms: 0, # defaults to 0
)
```

### Retention types

Memphis currently supports the following types of retention:

- message_age_sec: means that every message persists for the value set in retention value field (in seconds)

- messages: means that after max amount of saved messages (set in retention value), the oldest messages will be deleted

- bytes: means that after max amount of saved bytes (set in retention value), the oldest messages will be deleted

### Storage types

Memphis currently supports the following types of messages storage:

- file: means that messages persist on the file system

- memory: means that messages persist on the main memory

### Destroying a Station

Destroying a station will remove all its resources (producers/consumers)

```python
station.destroy()
```

### Produce and Consume messages

The most common client operations are `produce` to send messages and `consume` to
receive messages.

Messages are published to a station and consumed from it by creating a consumer.
Consumers are pull based and consume all the messages in a station unless you are using a consumers group, in this case messages are spread across all members in this group.

Memphis messages are payload agnostic. Payloads are `Uint8Arrays`.

In order to stop getting messages, you have to call `consumer.destroy()`. Destroy will terminate regardless
of whether there are messages in flight for the client.

### Creating a Producer

```python
producer = await memphis.producer(station_name="<station-name>", producer_name="<producer-name>")
```

### Producing a message

```python
await producer.produce({
  message: "<bytes array>", // Uint8Arrays
  ackWaitSec: 15, // defaults to 15
});

await producer.produce(message="<bytes array>", # Uint8Arrays
                       ack_WaitSec=15, # defaults to 15
                       )
```

### Destroying a Producer

```python
producer.destroy()
```

### Creating a Consumer

```python
  consumer = await memphis.consumer(
  stationName="<station-name>",
  consumerName="<consumer-name>",
  consumerGroup="<group-name>", # defaults to ""
  pull_interval_ms=1000, # defaults to 1000
  batch_size=10, # defaults to 10
  batch_max_time_to_wait_ms=5000, # defaults to 5000
  max_ack_time_ms=30000, # defaults to 30000
)
```

### Processing messages

```python
def msg_handler(msg):
    print("message: ", msg.get_data())
    msg.ack()

def error_handler(error):
    print("error: ", error)

consumer.event.on("message", msg_handler)
consumer.event.on("error", error_handler)
await consumer.consume()
```

### Acknowledge a message

Acknowledge a message indicates the Memphis server to not re-send the same message again to the same consumer / consumers group

```python
message.ack()
```

### Destroying a Consumer

```python
consumer.destroy()
```

## Memphis Contributors

<img src="https://memphis-public-files.s3.eu-central-1.amazonaws.com/contributors-images/Alon+Avrahami.jpg" width="60" height="60" style="border-radius: 25px; border: 2px solid #61DFC6;"> <img src="https://memphis-public-files.s3.eu-central-1.amazonaws.com/contributors-images/Ariel+Bar.jpeg" width="60" height="60" style="border-radius: 25px; border: 2px solid #61DFC6;"> <img src="https://memphis-public-files.s3.eu-central-1.amazonaws.com/contributors-images/Arjun+Anjaria.jpeg" width="60" height="60" style="border-radius: 25px; border: 2px solid #61DFC6;"> <img src="https://memphis-public-files.s3.eu-central-1.amazonaws.com/contributors-images/Carlos+Gasperi.jpeg" width="60" height="60" style="border-radius: 25px; border: 2px solid #61DFC6;"> <img src="https://memphis-public-files.s3.eu-central-1.amazonaws.com/contributors-images/Daniel+Eliyahu.jpeg" width="60" height="60" style="border-radius: 25px; border: 2px solid #61DFC6;"> <img src="https://memphis-public-files.s3.eu-central-1.amazonaws.com/contributors-images/Itay+Katz.jpeg" width="60" height="60" style="border-radius: 25px; border: 2px solid #61DFC6;"> <img src="https://memphis-public-files.s3.eu-central-1.amazonaws.com/contributors-images/Jim+Doty.jpeg" width="60" height="60" style="border-radius: 25px; border: 2px solid #61DFC6;"> <img src="https://memphis-public-files.s3.eu-central-1.amazonaws.com/contributors-images/Nikita+Aizenberg.jpg" width="60" height="60" style="border-radius: 25px; border: 2px solid #61DFC6;"> <img src="https://memphis-public-files.s3.eu-central-1.amazonaws.com/contributors-images/Rado+Marina.jpg" width="60" height="60" style="border-radius: 25px; border: 2px solid #61DFC6;"><img src="https://memphis-public-files.s3.eu-central-1.amazonaws.com/contributors-images/Raghav+Ramesh.jpg" width="60" height="60" style="border-radius: 25px; border: 2px solid #61DFC6;"> <img src="https://memphis-public-files.s3.eu-central-1.amazonaws.com/contributors-images/Tal+Goldberg.jpg" width="60" height="60" style="border-radius: 25px; border: 2px solid #61DFC6;"> <img src="https://memphis-public-files.s3.eu-central-1.amazonaws.com/contributors-images/Yehuda+Mizrahi.jpeg" width="60" height="60" style="border-radius: 25px; border: 2px solid #61DFC6;">

## Contribution guidelines

soon

## Documentation

- [Official documentation](https://docs.memphis.dev)

## Contact

- [Slack](https://bit.ly/37uwCPd): Q&A, Help, Feature requests, and more
- [Twitter](https://bit.ly/3xzkxTx): Follow us on Twitter!
- [Discord](https://bit.ly/3OfnuhX): Join our Discord Server!
- [Medium](https://bit.ly/3ryFDgS): Follow our Medium page!
- [Youtube](https://bit.ly/38Y8rcq): Subscribe our youtube channel!
