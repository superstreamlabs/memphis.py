<a href="![Github (4)](https://github.com/memphisdev/memphis-terraform/assets/107035359/a5fe5d0f-22e1-4445-957d-5ce4464e61b1)">![Github (4)](https://github.com/memphisdev/memphis-terraform/assets/107035359/a5fe5d0f-22e1-4445-957d-5ce4464e61b1)</a>
<p align="center">
<a href="https://memphis.dev/discord"><img src="https://img.shields.io/discord/963333392844328961?color=6557ff&label=discord" alt="Discord"></a>
<a href="https://github.com/memphisdev/memphis/issues?q=is%3Aissue+is%3Aclosed"><img src="https://img.shields.io/github/issues-closed/memphisdev/memphis?color=6557ff"></a> 
  <img src="https://img.shields.io/npm/dw/memphis-dev?color=ffc633&label=installations">
<a href="https://github.com/memphisdev/memphis/blob/master/CODE_OF_CONDUCT.md"><img src="https://img.shields.io/badge/Code%20of%20Conduct-v1.0-ff69b4.svg?color=ffc633" alt="Code Of Conduct"></a> 
<img alt="GitHub release (latest by date)" src="https://img.shields.io/github/v/release/memphisdev/memphis?color=61dfc6">
<img src="https://img.shields.io/github/last-commit/memphisdev/memphis?color=61dfc6&label=last%20commit">
</p>

<div align="center">
  
<img width="177" alt="cloud_native 2 (5)" src="https://github.com/memphisdev/memphis/assets/107035359/a20ea11c-d509-42bb-a46c-e388c8424101"> 

  <h4>

**[Memphis.dev](https://memphis.dev)** is a highly scalable, painless, and effortless data streaming platform.<br>
Made to enable developers and data teams to collaborate and build<br>
real-time and streaming apps fast.

  </h4>
  
</div>

## Installation

```sh
$ pip3 install memphis-py
```

Notice: you may receive an error about the "mmh3" package, to solve it please install python3-devel
```sh
$ sudo yum install python3-devel
```

## Importing

```python
from memphis import Memphis, Headers
from memphis.types import Retention, Storage
import asyncio
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
      account_id=<account_id>, # You can find it on the profile page in the Memphis UI. This field should be sent only on the cloud version of Memphis, otherwise it will be ignored
      connection_token="<broker-token>", # you will get it on application type user creation
      password="<string>", # depends on how Memphis deployed - default is connection token-based authentication
      port=<port>, # defaults to 6666
      reconnect=True, # defaults to True
      max_reconnect=10, # defaults to -1 which means reconnect indefinitely
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

```python
    async def connect(
        self,
        host: str,
        username: str,
        account_id: int = 1, # Cloud use only, ignored otherwise
        connection_token: str = "", # JWT token given when creating client accounts
        password: str = "", # For password-based connections
        port: int = 6666,
        reconnect: bool = True,
        max_reconnect: int = 10,
        reconnect_interval_ms: int = 1500,
        timeout_ms: int = 2000,
        # For TLS connections: 
        cert_file: str = "", 
        key_file: str = "",
        ca_file: str = "",
    )
```

The connect function in the Memphis class allows for the connection to Memphis. Connecting to Memphis (cloud or open-source) will be needed in order to use any of the other functionality of the Memphis class. Upon connection, all of Memphis' features are available.

What arguments are used with the Memphis.connect function change depending on the type of connection being made.

For details on deploying memphis open-source with different types of connections see the [docs](https://docs.memphis.dev/memphis/memphis-broker/concepts/security).
 
A password-based connection would look like this (using the defualt root memphis login with Memphis open-source):

```python
    # Imports hidden. See other examples
async def main():
  try:
      memphis = Memphis()
      await memphis.connect(
          host = "localhost",
          username = "root",
          password = "memphis",
          # port = 6666, default port
          # reconnect = True, default reconnect setting
          # max_reconnect = 10, default number of reconnect attempts
          # reconnect_interval_ms = 1500, default reconnect interval
          # timeout_ms = 2000, default duration of time for the connection to timeout
      )
  except Exception as e:
      print(e)
  finally:
      await memphis.close()

if __name__ == '__main__':
  asyncio.run(main())  
```

If you wanted to connect to Memphis cloud instead, simply add your account ID and change the host. The host and account_id can be found on the Overview page in the Memphis cloud UI under your name at the top. Here is an example to connecting to a cloud broker that is located in US East:  

```python
    # Imports hidden. See other examples
async def main():
  try:
      memphis = Memphis()
      await memphis.connect(
          host = "aws-us-east-1.cloud.memphis.dev",
          username = "my_client_username",
          password = "my_client_password",
          account_id = "123456789"
          # port = 6666, default port
          # reconnect = True, default reconnect setting
          # max_reconnect = 10, default number of reconnect attempts
          # reconnect_interval_ms = 1500, default reconnect interval
          # timeout_ms = 2000, default duration of time for the connection to timeout
      )
  except Exception as e:
      print(e)
  finally:
      await memphis.close()

if __name__ == '__main__':
  asyncio.run(main())  
```

It is possible to use a token-based connection to memphis as well, where multiple users can share the same token to connect to memphis. Here is an example of using memphis.connect with a token:

```python
    # Imports hidden. See other examples
async def main():
    try:
      memphis = Memphis()
      await memphis.connect(
          host = "localhost",
          username = "user",
          connection_token = "token",
          # port = 6666, default port
          # reconnect = True, default reconnect setting
          # max_reconnect = 10, default number of reconnect attempts
          # reconnect_interval_ms = 1500, default reconnect interval
          # timeout_ms = 2000, default duration of time for the connection to timeout
      )
  except Exception as e:
      print(e)
  finally:
      await memphis.close()

if __name__ == '__main__':
  asyncio.run(main())  
```

The token will be presented when creating new users. 

Memphis needs to be configured to use a token based connection. See the [docs](https://docs.memphis.dev/memphis/memphis-broker/concepts/security) for help doing this.

> For the rest of the examples, the try-except statement and the asyncio runtime call will be withheld to assist with the succinctness of the examples. 

A TLS based connection would look like this:

```python
    # Imports hidden. See other examples

    try:
        memphis = Memphis()
        await memphis.connect(
            host = "localhost",
            username = "user",
            key_file = "~/tls_file_path.key",
            cert_file = "~/tls_cert_file_path.crt",
            ca_file = "~/tls_ca_file_path.crt",
            # port = 6666, default port
            # reconnect = True, default reconnect setting
            # max_reconnect = 10, default number of reconnect attempts
            # reconnect_interval_ms = 1500, default reconnect interval
            # timeout_ms = 2000, default duration of time for the connection to timeout
        )
    except Exception as e:
        print(e)
    finally:
```
Memphis needs to configured for these use cases. To configure memphis to use TLS see the [docs](https://docs.memphis.dev/memphis/open-source-installation/kubernetes/production-best-practices#memphis-metadata-tls-connection-configuration).

### Disconnecting from Memphis

To disconnect from Memphis, call `close()` on the memphis object.

```python
await memphis.close()
```

### Creating a Station

Stations are distributed units that store messages. Producers add messages to stations and Consumers take messages from them. Each station stores messages until their retention policy causes them to either delete the messages or move them to [remote storage](https://docs.memphis.dev/memphis/integrations-center/storage/s3-compatible). 

**A station will be automatically created for the user when a consumer or producer is used if no stations with the given station name exist.**<br><br>
_If the station trying to be created exists when this function is called, nothing will change with the exisitng station_

```python
    async def station(
        self,
        name: str,
        retention_type: Retention = Retention.MAX_MESSAGE_AGE_SECONDS, # MAX_MESSAGE_AGE_SECONDS/MESSAGES/BYTES/ACK_BASED(cloud only). Defaults to MAX_MESSAGE_AGE_SECONDS
        retention_value: int = 3600, # defaults to 3600
        storage_type: Storage = Storage.DISK, # Storage.DISK/Storage.MEMORY. Defaults to DISK
        replicas: int = 1,
        idempotency_window_ms: int = 120000, # defaults to 2 minutes
        schema_name: str = "", # defaults to "" (no schema)
        send_poison_msg_to_dls: bool = True, # defaults to true
        send_schema_failed_msg_to_dls: bool = True, # defaults to true
        tiered_storage_enabled: bool = False, # defaults to false
        partitions_number: int = 1, # defaults to 1
        dls_station: str = "", # defaults to "" (no DLS station). If given, both poison and schema failed events will be sent to the DLS
    )
```

The station function is used to create a station. Using the different arguemnts, one can programically create many different types of stations. The Memphis UI can also be used to create stations to the same effect. 

Creating a station with just a name name would create a station with that named and containing the default options provided above:

```python
    memphis = Memphis()

    await memphis.connect(...)

    await memphis.station(
        name = "my_station"
    )
```

### Stations with Retention

To change what criteria the station uses to decide if a message should be retained in the station, change the retention type. The different types of retention are documented [here](https://github.com/memphisdev/memphis.py#retention-types) in the python README. 

The unit of the rentention value will vary depending on the retention_type. The [previous link](https://github.com/memphisdev/memphis.py#retention-types) also describes what units will be used. 

Here is an example of a station which will only hold up to 10 messages:

```python
    memphis = Memphis()

    await memphis.connect(...)
    
    await memphis.station(
        name = "my_station",
        retention_type = Retention.MESSAGES,
        retention_value = 10
    )
```

### Station storage types

Memphis stations can either store Messages on disk or in memory. A comparison of those types of storage can be found [here](https://docs.memphis.dev/memphis/memphis-broker/concepts/storage-and-redundancy#tier-1-local-storage).

Here is an example of how to create a station that uses Memory as its storage type:

```python
    memphis = Memphis()

    await memphis.connect(...)

    await memphis.station(
        name = "my_station",
        storage_type = Storage.MEMORY
    )
```

### Station Replicas

In order to make a station more redundant, replicas can be used. Read more about replicas [here](https://docs.memphis.dev/memphis/memphis-broker/concepts/storage-and-redundancy#replicas-mirroring). Note that replicas are only available in cluster mode. Cluster mode can be enabled in the [Helm settings](https://docs.memphis.dev/memphis/open-source-installation/kubernetes/1-installation#appendix-b-helm-deployment-options) when deploying Memphis with Kubernetes.

Here is an example of creating a station with 3 replicas:

```python
    memphis = Memphis()

    await memphis.connect(...)

    await memphis.station(
        name = "my_station",
        replicas = 3
    )
```

### Station idempotency

Idempotency defines how Memphis will prevent duplicate messages from being stored or consumed. The duration of time the message ID's will be stored in the station can be set with idempotency_window_ms. If the environment Memphis is deployed in has unreliably connection and/or a lot of latency, increasing this value might be desiriable. The default duration of time is set to two minutes. Read more about idempotency [here](https://docs.memphis.dev/memphis/memphis-broker/concepts/idempotency).

Here is an example of changing the idempotency window to 3 seconds:

```python
    memphis = Memphis()

    await memphis.connect(...)

    await memphis.station(
        name = "my_station",
        idempotency_window_ms = 180000
    )
```

### Enforcing a schema

The schema name is used to set a schema to be enforced by the station. The default value of "" ensures that no schema is enforced. Here is an example of changing the schema to a defined schema in schemaverse called "sensor_logs":

```python
    memphis = Memphis()

    await memphis.connect(...)

    await memphis.station(
        name = "my_station",
        schema = "sensor_logs"
    )
```

### Dead Letter Stations

There are two parameters for sending messages to the [dead-letter station(DLS)](https://docs.memphis.dev/memphis/memphis-broker/concepts/dead-letter#terminology). These are send_poison_msg_to_dls and send_schema_failed_msg_to_dls. 

Here is an example of sending poison messages to the DLS but not messages which fail to conform to the given schema.

```python
    memphis = Memphis()

    await memphis.connect(...)

    await memphis.station(
        name = "my_station",
        schema = "sensor_logs",
        send_poison_msg_to_dls = True,
        send_schema_failed_msg_to_dls = False
    )
```

When either of the DLS flags are set to True, a station can also be set to handle these events. To set a station as the station to where schema failed or poison messages will be set to, use the dls_station parameter:

```python
    memphis = Memphis()

    await memphis.connect(...)

    await memphis.station(
        name = "my_station",
        schema = "sensor_logs",
        send_poison_msg_to_dls = True,
        send_schema_failed_msg_to_dls = False,
        dls_station = "bad_sensor_messages_station"
    )
```

### Station Tiered Storage

When the retention value is met, Mempihs by default will delete old messages. If tiered storage is setup, Memphis can instead move messages to tier 2 storage. Read more about tiered storage [here](https://docs.memphis.dev/memphis/memphis-broker/concepts/storage-and-redundancy#storage-tiering). Enable this setting with the respective flag:

```python
    memphis = Memphis()

    await memphis.connect(...)

    await memphis.station(
        name = "my_station",
        tiered_storage_enabled = True
    )
```

### Station Partitions

[Partitioning](https://docs.memphis.dev/memphis/memphis-broker/concepts/station#partitions) might be useful for a station. To have a station partitioned, simply change the partitions number:

```python
    memphis = Memphis()

    await memphis.connect(...)

    await memphis.station(
        name = "my_station",
        partitions_number = 3
    )
```


### Retention types

Retention types define the methodology behind how a station behaves with its messages. Memphis currently supports the following retention types:

```python
memphis.types.Retention.MAX_MESSAGE_AGE_SECONDS
```

When the retention type is set to MAX_MESSAGE_AGE_SECONDS, messages will persist in the station for the number of seconds specified in the retention_value. 


```python
memphis.types.Retention.MESSAGES
```

When the retention type is set to MESSAGES, the station will only hold up to retention_value messages. The station will delete the oldest messsages to maintain a retention_value number of messages.

```python
memphis.types.Retention.BYTES
```

When the retention type is set to BYTES, the station will only hold up to retention_value BYTES. The oldest messages will be deleted in order to maintain at maximum retention_vlaue BYTES in the station.

```python
memphis.types.Retention.ACK_BASED # for cloud users only
```

When the retention type is set to ACK_BASED, messages in the station will be deleted after they are acked by all subscribed consumer groups.

### Retention Values

The unit of the `retention_value` changes depending on the `retention_type` specified. 

All retention values are of type `int`. The following units are used based on the respective retention type:

`memphis.types.Retention.MAX_MESSAGE_AGE_SECONDS` is **in seconds**,<br>
`memphis.types.Retention.MESSAGES` is a **number of messages**,<br>
`memphis.types.Retention.BYTES` is a **number of bytes**, <br>
With `memphis.ACK_BASED`, the `retention_type` is ignored 

### Storage types

Memphis currently supports the following types of messages storage:

```python
memphis.types.Storage.DISK
```
When storage is set to DISK, messages are stored on disk.

```python
memphis.types.Storage.MEMORY
```
When storage is set to MEMORY, messages are stored in the system memory.

### Destroying a Station

Destroying a station will remove all its resources (including producers/consumers)

```python
station.destroy()
```

### Creating a New Schema 
In case schema is already exist a new version will be created
```python
await memphis.create_schema("<schema-name>", "<schema-type>", "<schema-file-path>")
```
Current available schema types - Protobuf / JSON schema / GraphQL schema / Avro

### Enforcing a Schema on an Existing Station

```python
async def enforce_schema(self, name, station_name)
```

To add a schema to an already created station, enforce_schema can be used. Here is an example using enforce_schmea to add a schema to a station:

```python
    memphis = Memphis()

    await memphis.connect(...)

    await memphis.enforce_schmea(
        name = "my_schmea",
        station_name = "my_station"
    )
```

### Deprecated  - Attaching a Schema, use enforce_schema instead

```python
await memphis.attach_schema("<schema-name>", "<station-name>")
```

### Detaching a Schema from Station

```python
    async def detach_schema(self, station_name)
```

To remove a schema from an already created station, detach_schema can be used. Here is an example of removing a schmea from a station:

```python
    memphis = Memphis()
   
    await memphis.connect(...)

    await memphis.detach_schmea(
        station_name = "my_station"
    )
```

### Produce and Consume messages

The most common client operations are using `produce` to send messages and `consume` to
receive messages.

Messages are published to a station with a Producer and consumed from it by a Consumer. 

Consumers are poll based and consume all the messages in a station. Consumers can also be grouped into consumer groups. When consuming with a consumer group, all consumers in the group will receive each message.

Memphis messages are payload agnostic. Payloads are always `bytearray`s.

In order to stop getting messages, you have to call `consumer.destroy()`. Destroy will terminate the consumer even if messages are currently being sent to the consumer.

If a station is created with more than one partition, producing to and consuming from the station will happen in a round robin fashion. 

### Creating a Producer

```python
    async def producer(
        self,
        station_name: str,
        producer_name: str,
        generate_random_suffix: bool = False, #Depreicated
    )
```

Use the Memphis producer function to create a producer. Here is an example of creating a producer for a given station:

```python
    memphis = Memphis()
   
    await memphis.connect(...)

    producer = await memphis.producer(
        station_name = "my_station",
        producer_name = "new_producer"
    )
```

### Producing a message
```python
async def produce(
        self,
        message,
        ack_wait_sec: int = 15,
        headers: Union[Headers, None] = None,
        async_produce: Union[bool, None] = None,
        nonblocking: bool = False,
        msg_id: Union[str, None] = None,
        concurrent_task_limit: Union[int, None] = None,
        producer_partition_key: Union[str, None] = None,
        producer_partition_number: Union[int, -1] = -1
    ):
```
Both producers and connections can use the produce function. To produce a message from a connection, simply call `memphis.produce`. This function will create a producer if none with the given name exists, otherwise it will pull the producer from a cache and use it to produce the message.
 
```python
await memphis.produce(station_name='test_station_py', producer_name='prod_py',
  message='bytearray/protobuf class/dict/string/graphql.language.ast.DocumentNode', # bytearray / protobuf class (schema validated station - protobuf) or bytearray/dict (schema validated station - json schema) or string/bytearray/graphql.language.ast.DocumentNode (schema validated station - graphql schema) or bytearray/dict (schema validated station - avro schema)
  ack_wait_sec=15, # defaults to 15
  headers=headers, # default to {}
  nonblocking=False, #defaults to false
  msg_id="123",
  producer_partition_key="key" #default to None
)
```

Creating a producer and calling produce on it will increase the performance of producing messages as it removes the overhead of pulling created producers from the cache.

```python
await producer.produce(
  message='bytearray/protobuf class/dict/string/graphql.language.ast.DocumentNode', # bytearray / protobuf class (schema validated station - protobuf) or bytearray/dict (schema validated station - json schema) or string/bytearray/graphql.language.ast.DocumentNode (schema validated station - graphql schema) or or bytearray/dict (schema validated station - avro schema)
  ack_wait_sec=15) # defaults to 15
```

Here is an example of a produce function call that waits up to 30 seconds for an acknowledgement from memphis and does so in an nonblocking manner:

```python
   memphis = Memphis()

    await memphis.connect(...)
    
    await memphis.produce(
        station_name = "some_station",
        producer_name = "temp_producer",
        message = {'some':'message'},
        ack_wait_sec = 30,
        nonblocking = True
    )
```

### Producing with idempotency

As discussed before in the station section, idempotency is an important feature of memphis. To achieve idempotency, an id must be assigned to messages that are being produced. Use the msg_id parameter for this purpose.

```python
   memphis = Memphis()

    await memphis.connect(...)
    
    await memphis.produce(
        station_name = "some_station",
        producer_name = "temp_producer",
        message = {'some':'message'},
        msg_id = '42'
    )
```

### Producing with headers

To add message headers to the message, use the headers parameter. Headers can help with observability when using certain 3rd party to help monitor the behavior of memphis. See [here](https://docs.memphis.dev/memphis/memphis-broker/comparisons/aws-sqs-vs-memphis#observability) for more details.

```python
   memphis = Memphis()

   await memphis.connect(...)
    
   await memphis.produce(
       station_name = "some_station",
       producer_name = "temp_producer",
       message = {'some':'message'},
       headers = {
           'trace_header': 'track_me_123'
       }
   )
```

### Producing to a partition

Lastly, memphis can produce to a specific partition in a station. To do so, use the producer_partition_key parameter:

```python
   memphis = Memphis()

   await memphis.connect(...)
    
   await memphis.produce(
       station_name = "some_station",
       producer_name = "temp_producer",
       message = {'some':'message'},
       producer_partition_key = "2nd_partition"
   )
```

Or, alternatively, use the producer_partition_number parameter:
```python
   memphis = Memphis()

   await memphis.connect(...)
    
   await memphis.produce(
        station_name = "some_station",
        producer_name = "temp_producer",
        message = {'some':'message'},
        producer_partition_number = 2
   )
```

### Non-blocking Produce with Task Limits

For better performance, the client won't block requests while waiting for an acknowledgment.
If you are producing a large number of messages very quickly, there maybe some timeout errors, then you may need to limit the number of concurrent tasks to get around this:

```python
await producer.produce(
  message='bytearray/protobuf class/dict/string/graphql.language.ast.DocumentNode', # bytearray / protobuf class (schema validated station - protobuf) or bytearray/dict (schema validated station - json schema) or string/bytearray/graphql.language.ast.DocumentNode (schema validated station - graphql schema)
  headers={}, nonblocking=True, limit_concurrent_tasks=500)
```

You may read more about this [here](https://memphis.dev/blog/producing-messages-at-warp-speed-best-practices-for-optimizing-your-producers/) on the memphis.dev blog.

### Produce to multiple stations

Producing to multiple stations can be done by creating a producer with multiple stations and then calling produce on that producer.

```python
memphis = Memphis()

await memphis.connect(...)

producer = await memphis.producer(
  station_name = ["station_1", "station_2"],
  producer_name = "new_producer"
)

await producer.produce(
  message = "some message"
)
```

Alternatively, it also possible to produce to multiple stations using the connection:

```python
memphis = Memphis()

await memphis.connect(...)

await memphis.produce(
  station_name = ["station_1", "station_2"],
  producer_name = "new_producer",
  message = "some message"
)
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
  max_msg_deliveries=2, # defaults to 2
  start_consume_from_sequence=1, # start consuming from a specific sequence. defaults to 1
  last_messages=-1 # consume the last N messages, defaults to -1 (all messages in the station)
)
```

Consumers are used to pull messages from a station. Here is how to create a consumer with all of the default parameters:

```python
    memphis = Memphis()

    await memphis.connect(...)

    consumer = await Memphis.consumer(
        station_name = "my_station",
        consumer_name: "new_consumer",
    )
```

To create a consumer in a consumer group, add the consumer_group parameter:

```python
    memphis = Memphis()

    await memphis.connect(...)

    consumer = await Memphis.consumer(
        station_name = "my_station",
        consumer_name: "new_consumer",
        consumer_group: "consumer_group_1"
    )
```

When using Consumer.consume, the consumer will continue to consume in an infinite loop. To change the rate at which the consumer polls the station for new messages, change the pull_interval_ms parameter:

```python
    memphis = Memphis()

    await memphis.connect(...)

    consumer = await Memphis.consumer(
        station_name = "my_station",
        consumer_name = "new_consumer",
        pull_interval_ms = 2000
    )
```

Every time the consumer pulls from the station, the consumer will try to take batch_size number of elements from the station. However, sometimes there are not enough messages in the station for the consumer to consume a full batch. In this case, the consumer will continue to wait until either batch_size messages are gathered or the time in milliseconds specified by batch_max_time_to_wait_ms is reached. 

Here is an example of a consumer that will try to poll 100 messages every 10 seconds while waiting up to 15 seconds for all messages to reach the consumer.

```python
    memphis = Memphis()

    await memphis.connect(...)

    consumer = await Memphis.consumer(
        station_name = "my_station",
        consumer_name = "new_consumer",
        pull_interval_ms = 10000,
        batch_size = 100,
        batch_max_time_to_wait_ms = 15000
    )
```

The max_msg_deliveries parameter allows the user how many messages the consumer is able to consume before consuming more. The max_ack_time_ms Here is an example where the consumer will only hold up to one batch of messages at a time:

```python
    memphis = Memphis()

    await memphis.connect(...)
    
    consumer = await Memphis.consumer(
        station_name = "my_station",
        consumer_name = "new_consumer",
        pull_interval_ms = 10000,
        batch_size = 100,
        batch_max_time_to_wait_ms = 15000,
        max_msg_deliveries = 2
    )
```
### Consume using a partition key
The key will be used to consume from a specific partition

```python
consumer.consume(msg_handler,
                 consumer_partition_key = "key" #consume from a specific partition
                )
```

### Consume using a partition number
The number will be used to consume from a specific partition

```python
consumer.consume(msg_handler,
                 consumer_partition_number = -1 #consume from a specific partition
                )
```

### Setting a context for message handler function

```python
context = {"key": "value"}
consumer.set_context(context)
```

### Processing messages

To use a consumer to process messages, use the consume function. The consume function will have a consumer poll a station for new messages as discussed in previous sections. The consumer will stop polling the statoin once all the messages in the station were consumed, and the msg_handler will receive a `Memphis: TimeoutError`.

```python
async def msg_handler(msgs, error, context):
  for msg in msgs:
    print("message: ", msg.get_data())
    await msg.ack()
  if error:
    print(error)
consumer.consume(msg_handler)
```

#### Processing schema deserialized messages
To get messages deserialized, use `msg.get_data_deserialized()`.  

```python
async def msg_handler(msgs, error, context):
  for msg in msgs:
    print("message: ", await msg.get_data_deserialized())
    await msg.ack()
  if error:
    print(error)
consumer.consume(msg_handler)
```

There may be some instances where you apply a schema *after* a station has received some messages. In order to consume those messages get_data_deserialized may be used to consume the messages without trying to apply the schema to them. As an example, if you produced a string to a station and then attached a protobuf schema, using get_data_deserialized will not try to deserialize the string as a protobuf-formatted message.

### Fetch a single batch of messages

Using fetch_messages or fetch will allow the user to remove a specific number of messages from a given station. This behavior could be beneficial if the user does not want to have a consumer actively poll from a station indefinetly.

```python
msgs = await memphis.fetch_messages(
  station_name="<station-name>",
  consumer_name="<consumer-name>",
  consumer_group="<group-name>", # defaults to the consumer name
  batch_size=10, # defaults to 10
  batch_max_time_to_wait_ms=5000, # defaults to 5000
  max_ack_time_ms=30000, # defaults to 30000
  max_msg_deliveries=2, # defaults to 2
  start_consume_from_sequence=1, # start consuming from a specific sequence. defaults to 1
  last_messages=-1, # consume the last N messages, defaults to -1 (all messages in the station))
  consumer_partition_key="key", # used to consume from a specific partition, default to None 
  consumer_partition_number=-1 # used to consume from a specific partition, default to -1 
)
```

### Fetch a single batch of messages after creating a consumer
```python
msgs = await consumer.fetch(batch_size=10) # defaults to 10
```

### Fetch a single batch of messages after creating a consumer
`prefetch = true` will prefetch next batch of messages and save it in memory for future fetch() request<br>
```python
msgs = await consumer.fetch(batch_size=10, prefetch=True) # defaults to False
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
