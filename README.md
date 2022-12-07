<div align="center">
  
  ![Memphis light logo](https://github.com/memphisdev/memphis-broker/blob/master/logo-white.png?raw=true#gh-dark-mode-only)
  
</div>

<div align="center">
  
  ![Memphis light logo](https://github.com/memphisdev/memphis-broker/blob/master/logo-black.png?raw=true#gh-light-mode-only)
  
</div>

<div align="center">
<h4>Simple as RabbitMQ, Robust as Apache Kafka, and Perfect for microservices.</h4>

<img width="750" alt="Memphis UI" src="https://user-images.githubusercontent.com/70286779/204081372-186aae7b-a387-4253-83d1-b07dff69b3d0.png"><br>

  
  <a href="https://landscape.cncf.io/?selected=memphis"><img width="200" alt="CNCF Silver Member" src="https://github.com/cncf/artwork/raw/master/other/cncf-member/silver/white/cncf-member-silver-white.svg#gh-dark-mode-only"></a>
  
</div>

<div align="center">
  
  <img width="200" alt="CNCF Silver Member" src="https://github.com/cncf/artwork/raw/master/other/cncf-member/silver/color/cncf-member-silver-color.svg#gh-light-mode-only">
  
</div>
 
 <p align="center">
  <a href="https://sandbox.memphis.dev/" target="_blank">Sandbox</a> - <a href="https://memphis.dev/docs/">Docs</a> - <a href="https://twitter.com/Memphis_Dev">Twitter</a> - <a href="https://www.youtube.com/channel/UCVdMDLCSxXOqtgrBaRUHKKg">YouTube</a>
</p>

<p align="center">
<a href="https://discord.gg/WZpysvAeTf"><img src="https://img.shields.io/discord/963333392844328961?color=6557ff&label=discord" alt="Discord"></a> <a href=""><img src="https://img.shields.io/github/issues-closed/memphisdev/memphis-broker?color=6557ff"></a> <a href="https://github.com/memphisdev/memphis-broker/blob/master/CODE_OF_CONDUCT.md"><img src="https://img.shields.io/badge/Code%20of%20Conduct-v1.0-ff69b4.svg?color=ffc633" alt="Code Of Conduct"></a> <a href="https://github.com/memphisdev/memphis-broker/blob/master/LICENSE"><img src="https://img.shields.io/github/license/memphisdev/memphis-broker?color=ffc633"></a> <img alt="GitHub release (latest by date)" src="https://img.shields.io/github/v/release/memphisdev/memphis-broker?color=61dfc6"> <img src="https://img.shields.io/github/last-commit/memphisdev/memphis-broker?color=61dfc6&label=last%20commit">
</p>

**[Memphis{dev}](https://memphis.dev)** is an open-source real-time data processing platform<br>
that provides end-to-end support for in-app streaming use cases using Memphis distributed message broker.<br>
Memphis' platform requires zero ops, enables rapid development, extreme cost reduction, <br>
eliminates coding barriers, and saves a great amount of dev time for data-oriented developers and data engineers.

## Installation

```sh
$ pip3 install memphis-py
```

## Importing

```python
from memphis import Memphis, Headers
from memphis import retention_types, storage_types
```

### Connecting to Memphis

First, we need to create Memphis `object` and then connect with Memphis by using `memphis.connect`.

```python
async def main():
  try:
    memphis = Memphis()
    await memphis.connect(
      host="<memphis-host>",
      username="<application-type username>",
      connection_token="<broker-token>",
      port="<port>", # defaults to 6666
      reconnect=True, # defaults to True
      max_reconnect=3, # defaults to 3
      reconnect_interval_ms=1500, # defaults to 1500
      timeout_ms=1500 # defaults to 1500
      )
    ...
  except Exception as e:
    print(e)
  finally:
    await memphis.close()

if __name__ == '__main__':
  asyncio.run(main())
```

Once connected, the entire functionalities offered by Memphis are available.

### Disconnecting from Memphis

To disconnect from Memphis, call `close()` on the memphis object.

```python
await memphis.close()
```

### Creating a Station

_If a station already exists nothing happens, the new configuration will not be applied_

```python
station = memphis.station(
  name="<station-name>",
  schema_name="<schema-name>",
  retention_type=retention_types.MAX_MESSAGE_AGE_SECONDS, # MAX_MESSAGE_AGE_SECONDS/MESSAGES/BYTES. Defaults to MAX_MESSAGE_AGE_SECONDS
  retention_value=604800, # defaults to 604800
  storage_type=storage_types.DISK, # storage_types.DISK/storage_types.MEMORY. Defaults to DISK
  replicas=1, # defaults to 1
  idempotency_window_ms: 120000, # defaults to 2 minutes
)
```

### Retention types

Memphis currently supports the following types of retention:

```python
memphis.retention_types.MAX_MESSAGE_AGE_SECONDS
```

Means that every message persists for the value set in retention value field (in seconds)

```python
memphis.retention_types.MESSAGES
```

Means that after max amount of saved messages (set in retention value), the oldest messages will be deleted

```python
memphis.retention_types.BYTES
```

Means that after max amount of saved bytes (set in retention value), the oldest messages will be deleted

### Storage types

Memphis currently supports the following types of messages storage:

```python
memphis.storage_types.DISK
```

Means that messages persist on disk

```python
memphis.storage_types.MEMORY
```

Means that messages persist on the main memory

### Destroying a Station

Destroying a station will remove all its resources (producers/consumers)

```python
station.destroy()
```

### Attaching a Schema to an Existing Station

```python
await memphis.attach_schema("<schema-name>", "<station-name>")
```

### Detaching a Schema from Station

```python
await memphis.detach_schema("<station-name>")
```


### Produce and Consume messages

The most common client operations are `produce` to send messages and `consume` to
receive messages.

Messages are published to a station and consumed from it by creating a consumer.
Consumers are pull based and consume all the messages in a station unless you are using a consumers group, in this case messages are spread across all members in this group.

Memphis messages are payload agnostic. Payloads are `bytearray`.

In order to stop getting messages, you have to call `consumer.destroy()`. Destroy will terminate regardless
of whether there are messages in flight for the client.

### Creating a Producer

```python
producer = await memphis.producer(station_name="<station-name>", producer_name="<producer-name>", generate_random_suffix=False)
```

### Producing a message

```python
await prod.produce(
  message='bytearray/protobuf class/dict', # bytes / protobuf class (schema validated station - protobuf) or bytes/dict (schema validated station - json schema)
  ack_wait_sec=15) # defaults to 15
```

### Add headers

```python
headers= Headers()
headers.add("key", "value")
await producer.produce(
  message='bytearray/protobuf class/dict', # bytes / protobuf class (schema validated station - protobuf) or bytes/dict (schema validated station - json schema)
  headers=headers) # default to {}
```

### Async produce
Meaning your application won't wait for broker acknowledgement - use only in case you are tolerant for data loss

```python
await producer.produce(
  message='bytearray/protobuf class/dict', # bytes / protobuf class (schema validated station - protobuf) or bytes/dict (schema validated station - json schema)
  headers={}, async_produce=True)
```

### Message ID
Stations are idempotent by default for 2 minutes (can be configured), Idempotency achieved by adding a message id

```python
await producer.produce(
  message='bytearray/protobuf class/dict', # bytes / protobuf class (schema validated station - protobuf) or bytes/dict (schema validated station - json schema)
  headers={}, 
  async_produce=True,
  msg_id="123")
```

### Destroying a Producer

```python
producer.destroy()
```

### Creating a Consumer

```python
consumer = await memphis.consumer(
  station_name="<station-name>",
  consumer_name="<consumer-name>",
  consumer_group="<group-name>", # defaults to the consumer name
  pull_interval_ms=1000, # defaults to 1000
  batch_size=10, # defaults to 10
  batch_max_time_to_wait_ms=5000, # defaults to 5000
  max_ack_time_ms=30000, # defaults to 30000
  max_msg_deliveries=10, # defaults to 10
  generate_random_suffix=False
)
```

### Processing messages

Once all the messages in the station were consumed the msg_handler will receive error: `Memphis: TimeoutError`.

```python
async def msg_handler(msgs, error):
  for msg in msgs:
    print("message: ", msg.get_data())
    await msg.ack()
  if error:
    print(error)
consumer.consume(msg_handler)
```

### Acknowledge a message

Acknowledge a message indicates the Memphis server to not re-send the same message again to the same consumer / consumers group

```python
await message.ack()
```

### Get headers 
Get headers per message

``python
headers = message.get_headers()
```

### Destroying a Consumer

```python
consumer.destroy()
```
