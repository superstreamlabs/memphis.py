from __future__ import annotations

import asyncio
import json
import time
from typing import Union
import warnings

import graphql
from graphql import parse as parse_graphql
from graphql import validate as validate_graphql
from jsonschema import validate
import google.protobuf.json_format as protobuf_json_format
import fastavro
import mmh3
from memphis.exceptions import MemphisError, MemphisSchemaError
from memphis.headers import Headers
from memphis.utils import get_internal_name
from memphis.partition_generator import PartitionGenerator

schemaverse_fail_alert_type = "schema_validation_fail_alert"


class Producer:
    def __init__(
        self, connection, producer_name: str, station_name: str, real_name: str
    ):
        self.connection = connection
        self.producer_name = producer_name.lower()
        self.station_name = station_name
        self.internal_station_name = get_internal_name(self.station_name)
        self.loop = asyncio.get_running_loop()
        self.real_name = real_name
        self.background_tasks = set()
        if self.internal_station_name in connection.partition_producers_updates_data:
            self.partition_generator = PartitionGenerator(connection.partition_producers_updates_data[self.internal_station_name]["partitions_list"])

    async def validate_msg(self, message):
        if self.connection.schema_updates_data[self.internal_station_name] != {}:
            schema_type = self.connection.schema_updates_data[
                self.internal_station_name
            ]["type"]
            if schema_type == "protobuf":
                message = self.validate_protobuf(message)
                return message
            if schema_type == "json":
                message = self.validate_json_schema(message)
                return message
            if schema_type == "graphql":
                message = self.validate_graphql(message)
                return message
            if schema_type == "avro":
                message = self.validate_avro_schema(message)
                return message
            if hasattr(message, "SerializeToString"):
                msg_to_send = message.SerializeToString()
                return msg_to_send
        elif isinstance(message, str):
            message = message.encode("utf-8")
            return message
        elif isinstance(message, graphql.language.ast.DocumentNode):
            msg = message
            message = str(msg.loc.source.body)
            message = message.encode("utf-8")
            return message
        elif hasattr(message, "SerializeToString"):
            msg_to_send = message.SerializeToString()
            return msg_to_send
        elif not isinstance(message, bytearray) and not isinstance(message, dict):
            raise MemphisSchemaError("Unsupported message type")
        else:
            if isinstance(message, dict):
                message = bytearray(json.dumps(message).encode("utf-8"))
            return message

    def validate_protobuf(self, message):
        proto_msg = self.connection.proto_msgs[self.internal_station_name]
        msg_to_send = ""
        try:
            if isinstance(message, bytearray):
                msg_to_send = bytes(message)
                try:
                    proto_msg.ParseFromString(msg_to_send)
                    proto_msg.SerializeToString()
                    msg_to_send = msg_to_send.decode("utf-8")
                except Exception as e:
                    if "parsing message" in str(e):
                        e = "Invalid message format, expecting protobuf"
                    raise MemphisSchemaError(str(e))
                return message
            if hasattr(message, "SerializeToString"):
                msg_to_send = message.SerializeToString()
                proto_msg.ParseFromString(msg_to_send)
                proto_msg.SerializeToString()
                try:
                    proto_msg.ParseFromString(msg_to_send)
                    proto_msg.SerializeToString()
                except Exception as e:
                    if "parsing message" in str(e):
                        e = "Error parsing protobuf message"
                    raise MemphisSchemaError(str(e))
                return msg_to_send
            elif isinstance(message, dict):
                try:
                    protobuf_json_format.ParseDict(message, proto_msg)
                    msg_to_send = proto_msg.SerializeToString()
                    return msg_to_send
                except Exception as e:
                    raise MemphisSchemaError(str(e))
            else:
                raise MemphisSchemaError("Unsupported message type")

        except Exception as e:
            raise MemphisSchemaError("Schema validation has failed: " + str(e))

    def validate_json_schema(self, message):
        try:
            if isinstance(message, bytearray):
                try:
                    message_obj = json.loads(message)
                except Exception as e:
                    raise Exception("Expecting Json format: " + str(e))
            elif isinstance(message, dict):
                message_obj = message
                message = bytearray(json.dumps(message_obj).encode("utf-8"))
            else:
                raise Exception("Unsupported message type")

            validate(
                instance=message_obj,
                schema=self.connection.json_schemas[self.internal_station_name],
            )
            return message
        except Exception as e:
            raise MemphisSchemaError("Schema validation has failed: " + str(e))

    def validate_graphql(self, message):
        try:
            if isinstance(message, bytearray):
                msg = message.decode("utf-8")
                msg = parse_graphql(msg)
            elif isinstance(message, str):
                msg = parse_graphql(message)
                message = message.encode("utf-8")
            elif isinstance(message, graphql.language.ast.DocumentNode):
                msg = message
                message = str(msg.loc.source.body)
                message = message.encode("utf-8")
            else:
                raise Exception("Unsupported message type")
            validate_res = validate_graphql(
                schema=self.connection.graphql_schemas[self.internal_station_name],
                document_ast=msg,
            )
            if len(validate_res) > 0:
                raise Exception(
                    "Schema validation has failed: " + str(validate_res))
            return message
        except Exception as e:
            if "Syntax Error" in str(e):
                e = "Invalid message format, expected GraphQL"
            raise MemphisSchemaError("Schema validation has failed: " + str(e))


    def validate_avro_schema(self, message):
        try:
            if isinstance(message, bytearray):
                try:
                    message_obj = json.loads(message)
                except Exception as e:
                    raise Exception("Expecting Avro format: " + str(e))
            elif isinstance(message, dict):
                message_obj = message
                message = bytearray(json.dumps(message_obj).encode("utf-8"))
            else:
                raise Exception("Unsupported message type")

            fastavro.validate(
                message_obj,
                self.connection.avro_schemas[self.internal_station_name],
            )
            return message
        except fastavro.validation.ValidationError as e:
            raise MemphisSchemaError("Schema validation has failed: " + str(e))

    # pylint: disable=R0913
    async def produce(
        self,
        message,
        ack_wait_sec: int = 15,
        headers: Union[Headers, None] = None,
        async_produce: Union[bool, None] = None,
        nonblocking: bool = False,
        msg_id: Union[str, None] = None,
        concurrent_task_limit: Union[int, None] = None,
        producer_partition_key: Union[str, None] = None
    ):
        """Produces a message into a station.
        Args:
            message (bytearray/dict): message to send into the station
                                      - bytearray/protobuf class
                                        (schema validated station - protobuf)
                                      - bytearray/dict (schema validated station - json schema)
                                      - string/bytearray/graphql.language.ast.DocumentNode
                                        (schema validated station - graphql schema)
                                      - bytearray/dict (schema validated station - avro schema)
            ack_wait_sec (int, optional): max time in seconds to wait for an ack from the broker.
                                          Defaults to 15 sec.
            headers (dict, optional): message headers, defaults to {}.
            async_produce (boolean, optional): produce operation won't block (wait) on message send.
                                               This argument is deprecated. Please use the
                                               `nonblocking` arguemnt instead.
            nonblocking (boolean, optional): produce operation won't block (wait) on message send.
            msg_id (string, optional): Attach msg-id header to the message in order to
                                       achieve idempotency.
            concurrent_task_limit (int, optional): Limit the number of outstanding async produce
                                                   tasks. Calls with nonblocking=True will block
                                                   if the limit is hit and will wait until the
                                                   buffer drains halfway down.
            producer_partition_key (string, optional): Produce messages to a specific partition using the partition key.
        Raises:
            Exception: _description_
        """
        try:
            message = await self.validate_msg(message)

            memphis_headers = {
                "$memphis_producedBy": self.producer_name,
                "$memphis_connectionId": self.connection.connection_id,
            }

            if msg_id is not None and msg_id != "":
                memphis_headers["msg-id"] = msg_id

            if headers is not None:
                headers = headers.headers
                headers.update(memphis_headers)
            else:
                headers = memphis_headers

            if self.internal_station_name not in self.connection.partition_producers_updates_data:
                partition_name = self.internal_station_name
            elif len(self.connection.partition_producers_updates_data[self.internal_station_name]['partitions_list']) == 1:
                partition_name = f"{self.internal_station_name}${self.connection.partition_producers_updates_data[self.internal_station_name]['partitions_list'][0]}"
            elif producer_partition_key is not None:
                partition_number = self.get_partition_from_key(producer_partition_key)
                partition_name = f"{self.internal_station_name}${str(partition_number)}"
            else:
                partition_name = f"{self.internal_station_name}${str(next(self.partition_generator))}"

            if async_produce:
                nonblocking = True
                warnings.warn("The argument async_produce is deprecated. " + \
                              "Please use the argument nonblocking instead.")

            if nonblocking:
                try:
                    task = self.loop.create_task(
                               self.connection.broker_connection.publish(
                                 partition_name + ".final",
                                 message,
                                 timeout=ack_wait_sec,
                                 headers=headers,
                               )
                           )
                    self.background_tasks.add(task)
                    task.add_done_callback(self.background_tasks.discard)

                    # block until the number of outstanding async tasks is reduced
                    if concurrent_task_limit is not None and \
                        len(self.background_tasks) >= concurrent_task_limit:
                        desired_size = concurrent_task_limit / 2
                        while len(self.background_tasks) > desired_size:
                            await asyncio.sleep(0.1)

                    await asyncio.sleep(0)
                except Exception as e:
                    raise MemphisError(e)
            else:
                await self.connection.broker_connection.publish(
                    partition_name + ".final",
                    message,
                    timeout=ack_wait_sec,
                    headers=headers,
                )
        except MemphisSchemaError as e:
            if self.connection.schema_updates_data[self.internal_station_name] != {}:
                msg_to_send = ""
                if hasattr(message, "SerializeToString"):
                    msg_to_send = message.SerializeToString().decode("utf-8")
                elif isinstance(message, bytearray):
                    msg_to_send = str(message, "utf-8")
                else:
                    msg_to_send = str(message)

                if self.connection.station_schemaverse_to_dls[
                    self.internal_station_name
                ]:
                    unix_time = int(time.time())

                    memphis_headers = {
                        "$memphis_producedBy": self.producer_name,
                        "$memphis_connectionId": self.connection.connection_id,
                    }

                    if headers != {} and not headers == None:
                        headers = headers.headers
                        headers.update(memphis_headers)
                    else:
                        headers = memphis_headers

                    msg_to_send_encoded = msg_to_send.encode("utf-8")
                    msg_hex = msg_to_send_encoded.hex()
                    buf = {
                        "station_name": self.internal_station_name,
                        "producer": {
                            "name": self.producer_name,
                            "connection_id": self.connection.connection_id,
                        },
                        "creation_unix": unix_time,
                        "message": {
                            "data": msg_hex,
                            "headers": headers,
                        },
                        "validation_error": str(e)
                    }
                    buf = json.dumps(buf).encode("utf-8")
                    await self.connection.broker_manager.publish("$memphis_schemaverse_dls", buf)

                if self.connection.cluster_configurations.get(
                    "send_notification"
                ):
                    await self.connection.send_notification(
                        "Schema validation has failed",
                        "Station: "
                        + self.station_name
                        + "\nProducer: "
                        + self.producer_name
                        + "\nError:"
                        + str(e),
                        msg_to_send,
                        schemaverse_fail_alert_type,
                    )
            raise e
        except Exception as e: # pylint: disable-next=no-member
            if hasattr(e, "status_code") and e.status_code == "503":
                raise MemphisError(
                    "Produce operation has failed, please check whether Station/Producer still exist"
                )
            raise MemphisError(str(e)) from e

    async def destroy(self):
        """Destroy the producer."""
        try:
            # drain buffered async messages
            while len(self.background_tasks) > 0:
                await asyncio.sleep(0.1)

            destroy_producer_req = {
                "name": self.producer_name,
                "station_name": self.station_name,
                "username": self.connection.username,
                "connection_id": self.connection.connection_id,
                "req_version": 1,
            }

            producer_name = json.dumps(destroy_producer_req).encode("utf-8")
            res = await self.connection.broker_manager.request(
                "$memphis_producer_destructions", producer_name, timeout=5
            )
            error = res.data.decode("utf-8")
            if error != "" and not "not exist" in error:
                raise Exception(error)

            internal_station_name = get_internal_name(self.station_name)
            producer_number = (
                self.connection.producers_per_station.get(
                    internal_station_name) - 1
            )
            self.connection.producers_per_station[
                internal_station_name
            ] = producer_number

            if producer_number == 0:
                sub = self.connection.schema_updates_subs.get(
                    internal_station_name)
                task = self.connection.schema_tasks.get(internal_station_name)
                if internal_station_name in self.connection.schema_updates_data:
                    del self.connection.schema_updates_data[internal_station_name]
                if internal_station_name in self.connection.schema_updates_subs:
                    del self.connection.schema_updates_subs[internal_station_name]
                if internal_station_name in self.connection.schema_tasks:
                    del self.connection.schema_tasks[internal_station_name]
                if task is not None:
                    task.cancel()
                if sub is not None:
                    await sub.unsubscribe()

            map_key = internal_station_name + "_" + self.real_name
            del self.connection.producers_map[map_key]

        except Exception as e:
            raise Exception(e)

    def get_partition_from_key(self, key):
        try:
            index = mmh3.hash(key, self.connection.SEED, signed=False) % len(self.connection.partition_producers_updates_data[self.internal_station_name]["partitions_list"])
            return self.connection.partition_producers_updates_data[self.internal_station_name]["partitions_list"][index]
        except Exception as e:
            raise e
