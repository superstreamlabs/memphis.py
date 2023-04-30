<div align="center">
  
  ![github memphis banner](https://user-images.githubusercontent.com/70286779/229371212-8531c1e1-1a9d-4bbe-9285-b4dbb8601bfa.jpeg)
  
</div>

<div align="center">

  <h4>

**[Memphis](https://memphis.dev)** is a next-generation alternative to traditional message brokers.

  </h4>
  
  <a href="https://landscape.cncf.io/?selected=memphis"><img width="200" alt="CNCF Silver Member" src="https://github.com/cncf/artwork/raw/master/other/cncf-member/silver/white/cncf-member-silver-white.svg#gh-dark-mode-only"></a>
  
</div>

<div align="center">
  
  <img width="200" alt="CNCF Silver Member" src="https://github.com/cncf/artwork/raw/master/other/cncf-member/silver/color/cncf-member-silver-color.svg#gh-light-mode-only">
  
</div>
 
 <p align="center">
  <a href="https://memphis.dev/docs/">Docs</a> - <a href="https://twitter.com/Memphis_Dev">Twitter</a> - <a href="https://www.youtube.com/channel/UCVdMDLCSxXOqtgrBaRUHKKg">YouTube</a>
</p>

<p align="center">
<a href="https://discord.gg/WZpysvAeTf"><img src="https://img.shields.io/discord/963333392844328961?color=6557ff&label=discord" alt="Discord"></a>
<a href="https://github.com/memphisdev/memphis/issues?q=is%3Aissue+is%3Aclosed"><img src="https://img.shields.io/github/issues-closed/memphisdev/memphis?color=6557ff"></a> 
  <img src="https://img.shields.io/npm/dw/memphis-dev?color=ffc633&label=installations">
<a href="https://github.com/memphisdev/memphis/blob/master/CODE_OF_CONDUCT.md"><img src="https://img.shields.io/badge/Code%20of%20Conduct-v1.0-ff69b4.svg?color=ffc633" alt="Code Of Conduct"></a> 
<a href="https://docs.memphis.dev/memphis/release-notes/releases/v0.4.2-beta"><img alt="GitHub release (latest by date)" src="https://img.shields.io/github/v/release/memphisdev/memphis?color=61dfc6"></a>
<img src="https://img.shields.io/github/last-commit/memphisdev/memphis?color=61dfc6&label=last%20commit">
</p>

A simple, robust, and durable cloud-native message broker wrapped with<br>
an entire ecosystem that enables cost-effective, fast, and reliable development of modern queue-based use cases.<br><br>
Memphis enables the building of modern queue-based applications that require<br>
large volumes of streamed and enriched data, modern protocols, zero ops, rapid development,<br>
extreme cost reduction, and a significantly lower amount of dev time for data-oriented developers and data engineers.

## Installation

```sh
$ pip3 install memphis-py
```

## Importing

```python
from memphis import Memphis, Headers
from memphis.types import Retention, Storage
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
      account_id="<account_id>", # You can find it on the profile page in the Memphis UI. This field should be sent only on the cloud version of Memphis, otherwise it will be ignored
      connection_token="<broker-token>", # you will get it on application type user creation
      password="<string>", # depends on how Memphis deployed - default is connection token-based authentication
      port="<port>", # defaults to 6666
      reconnect=True, # defaults to True
      max_reconnect=3, # defaults to 3
      reconnect_interval_ms=1500, # defaults to 1500
      timeout_ms=1500, # defaults to 1500
      # for TLS connection:
      key_file='<key-client.pem>', 
      cert_file='<cert-client.pem>', 
      ca_file='<rootCA.pem>'
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
**Unexist stations will be created automatically through the SDK on the first producer/consumer connection with default values.**<br><br>
_If a station already exists nothing happens, the new configuration will not be applied_

```python
station = memphis.station(
  name="<station-name>",
  schema_name="<schema-name>",
  retention_type=Retention.MAX_MESSAGE_AGE_SECONDS, # MAX_MESSAGE_AGE_SECONDS/MESSAGES/BYTES. Defaults to MAX_MESSAGE_AGE_SECONDS
  retention_value=604800, # defaults to 604800
  storage_type=Storage.DISK, # Storage.DISK/Storage.MEMORY. Defaults to DISK
  replicas=1, # defaults to 1
  idempotency_window_ms=120000, # defaults to 2 minutes
  send_poison_msg_to_dls=True, # defaults to true
  send_schema_failed_msg_to_dls=True, # defaults to true
  tiered_storage_enabled=False # defaults to false
)
```

### Retention types

Memphis currently supports the following types of retention:

```python
memphis.types.Retention.MAX_MESSAGE_AGE_SECONDS
```

Means that every message persists for the value set in retention value field (in seconds)

```python
memphis.types.Retention.MESSAGES
```

Means that after max amount of saved messages (set in retention value), the oldest messages will be deleted

```python
memphis.types.Retention.BYTES
```

Means that after max amount of saved bytes (set in retention value), the oldest messages will be deleted


### Retention Values

The `retention values` are directly related to the `retention types` mentioned above, where the values vary according to the type of retention chosen.

All retention values are of type `int` but with different representations as follows:

`memphis.types.Retention.MAX_MESSAGE_AGE_SECONDS` is represented **in seconds**, `memphis.types.Retention.MESSAGES` in a **number of messages** and finally `memphis.types.Retention.BYTES` in a **number of bytes**.

After these limits are reached oldest messages will be deleted.

### Storage types

Memphis currently supports the following types of messages storage:

```python
memphis.types.Storage.DISK
```

Means that messages persist on disk

```python
memphis.types.Storage.MEMORY
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
Without creating a producer.
In cases where extra performance is needed the recommended way is to create a producer first
and produce messages by using the produce function of it
```python
await memphis.produce(station_name='test_station_py', producer_name='prod_py',
  message='bytearray/protobuf class/dict/string/graphql.language.ast.DocumentNode', # bytearray / protobuf class (schema validated station - protobuf) or bytearray/dict (schema validated station - json schema) or string/bytearray/graphql.language.ast.DocumentNode (schema validated station - graphql schema) 
  generate_random_suffix=False, #defaults to false
  ack_wait_sec=15, # defaults to 15
  headers=headers, # default to {}
  async_produce=False, #defaults to false
  msg_id="123"
)
```


With creating a producer
```python
await producer.produce(
  message='bytearray/protobuf class/dict/string/graphql.language.ast.DocumentNode', # bytearray / protobuf class (schema validated station - protobuf) or bytearray/dict (schema validated station - json schema) or string/bytearray/graphql.language.ast.DocumentNode (schema validated station - graphql schema)
  ack_wait_sec=15) # defaults to 15
```

### Add headers

```python
headers= Headers()
headers.add("key", "value")
await producer.produce(
  message='bytearray/protobuf class/dict/string/graphql.language.ast.DocumentNode', # bytearray / protobuf class (schema validated station - protobuf) or bytearray/dict (schema validated station - json schema) or string/bytearray/graphql.language.ast.DocumentNode (schema validated station - graphql schema)
  headers=headers) # default to {}
```

### Async produce
Meaning your application won't wait for broker acknowledgement - use only in case you are tolerant for data loss

```python
await producer.produce(
  message='bytearray/protobuf class/dict/string/graphql.language.ast.DocumentNode', # bytearray / protobuf class (schema validated station - protobuf) or bytearray/dict (schema validated station - json schema) or string/bytearray/graphql.language.ast.DocumentNode (schema validated station - graphql schema)
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
  generate_random_suffix=False,
  start_consume_from_sequence=1, # start consuming from a specific sequence. defaults to 1
  last_messages=-1 # consume the last N messages, defaults to -1 (all messages in the station)
)
```

### Setting a context for message handler function

```python
context = {"key": "value"}
consumer.set_context(context)
```

### Processing messages

Once all the messages in the station were consumed the msg_handler will receive error: `Memphis: TimeoutError`.

```python
async def msg_handler(msgs, error, context):
  for msg in msgs:
    print("message: ", msg.get_data())
    await msg.ack()
  if error:
    print(error)
consumer.consume(msg_handler)
```

### Fetch a single batch of messages
```python
msgs = await memphis.fetch_messages(
  station_name="<station-name>",
  consumer_name="<consumer-name>",
  consumer_group="<group-name>", # defaults to the consumer name
  batch_size=10, # defaults to 10
  batch_max_time_to_wait_ms=5000, # defaults to 5000
  max_ack_time_ms=30000, # defaults to 30000
  max_msg_deliveries=10, # defaults to 10
  generate_random_suffix=False
  start_consume_from_sequence=1 # start consuming from a specific sequence. defaults to 1
  last_messages=-1 # consume the last N messages, defaults to -1 (all messages in the station))
)
```

### Fetch a single batch of messages after creating a consumer
```python
msgs = await consumer.fetch(batch_size=10) # defaults to 10
```


### Acknowledge a message

Acknowledge a message indicates the Memphis server to not re-send the same message again to the same consumer / consumers group

```python
await message.ack()
```

### Delay the message after a given duration

Delay the message and tell Memphis server to re-send the same message again to the same consumer group. The message will be redelivered only in case `consumer.max_msg_deliveries` is not reached yet.

```python
await message.delay(delay_in_seconds)
```

### Get headers 
Get headers per message

```python
headers = message.get_headers()
```

### Get message sequence number
Get message sequence number

```python
sequence_number = msg.get_sequence_number()
```

### Destroying a Consumer

```python
consumer.destroy()
```


### Check connection status

```python
memphis.is_connected()
```
