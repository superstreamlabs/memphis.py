import json
import graphql
from graphql import parse as parse_graphql
from graphql import validate as validate_graphql
from jsonschema import validate
import google.protobuf.json_format as protobuf_json_format
import fastavro

from memphis.exceptions import MemphisError, MemphisSchemaError
from memphis.utils import get_internal_name


class Station:
    def __init__(self, connection, name: str):
        self.connection = connection
        self.name = name.lower()
        self.internal_station_name = get_internal_name(self.name)

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

    async def destroy(self, timeout_retries=5):
        """Destroy the station."""
        try:
            name_req = {"station_name": self.name, "username": self.connection.username}
            station_name = json.dumps(name_req, indent=2).encode("utf-8")
            # pylint: disable=protected-access
            res = await self.connection._request(
                "$memphis_station_destructions", station_name, 5, timeout_retries
            )
            # pylint: enable=protected-access
            error = res.data.decode("utf-8")
            if error != "" and not "not exist" in error:
                raise MemphisError(error)

            internal_station_name = get_internal_name(self.name)
            sub = self.connection.schema_updates_subs.get(internal_station_name)
            task = self.connection.schema_tasks.get(internal_station_name)
            if internal_station_name in self.connection.schema_updates_data:
                del self.connection.schema_updates_data[internal_station_name]
            if internal_station_name in self.connection.schema_updates_subs:
                del self.connection.schema_updates_subs[internal_station_name]
            if internal_station_name in self.connection.clients_per_station:
                del self.connection.clients_per_station[internal_station_name]
            if internal_station_name in self.connection.schema_tasks:
                del self.connection.schema_tasks[internal_station_name]
            if task is not None:
                task.cancel()
            if sub is not None:
                await sub.unsubscribe()


            if internal_station_name in self.connection.functions_clients_per_station:
                del self.connection.functions_clients_per_station[internal_station_name]
            if internal_station_name in self.connection.functions_updates_data:
                del self.connection.functions_updates_data[internal_station_name]
            if internal_station_name in self.connection.functions_updates_subs:
                function_sub = self.connection.functions_updates_subs.get(internal_station_name)
                if function_sub is not None:
                    await function_sub.unsubscribe()
                del self.connection.functions_updates_subs[internal_station_name]
            if internal_station_name in self.connection.functions_tasks:
                task = self.connection.functions_tasks.get(internal_station_name)
                if task is not None:
                    task.cancel()
                del self.connection.functions_tasks[internal_station_name]

            self.connection.producers_map = {
                k: v
                for k, v in self.connection.producers_map.items()
                if self.name not in k
            }

            self.connection.consumers_map = {
                k: v
                for k, v in self.connection.consumers_map.items()
                if self.name not in k
            }

        except Exception as e:
            raise MemphisError(str(e)) from e
