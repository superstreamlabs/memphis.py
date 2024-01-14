from __future__ import annotations

import json

from memphis.exceptions import MemphisConnectError, MemphisError, MemphisSchemaError
from memphis.station import Station

class Message:
    def __init__(self, message, connection, cg_name, internal_station_name, partition = 0):
        self.message = message
        self.connection = connection
        self.cg_name = cg_name
        self.internal_station_name = internal_station_name
        self.partition = partition
        self.station = Station(connection, internal_station_name)

    async def ack(self):
        """Ack a message is done processing."""
        try:
            await self.message.ack()
        except Exception as e:
            if (
                "$memphis_pm_id" in self.message.headers
                and "$memphis_pm_cg_name" in self.message.headers
            ):
                try:
                    msg = {
                        "id": int(self.message.headers["$memphis_pm_id"]),
                        "cg_name": self.message.headers["$memphis_pm_cg_name"],
                    }
                    msg_to_ack = json.dumps(msg).encode("utf-8")
                    await self.connection.broker_manager.publish(
                        "$memphis_pm_acks", msg_to_ack
                    )
                except Exception as e_1:
                    raise MemphisConnectError(str(e_1))
            else:
                raise MemphisConnectError(str(e)) from e
            return

    async def nack(self):
        """
        nack - not ack for a message, meaning that the message will be redelivered again to the same consumers group without waiting to its ack wait time.
        """
        if not hasattr(self.message, 'nak'):
            return
        await self.message.nak()

    async def dead_letter(self, reason: str):
        """
        dead_letter - Sending the message to the dead-letter station (DLS). the broker won't resend the message again to the same consumers group and will place the message inside the dead-letter station (DLS) with the given reason.
        The message will still be available to other consumer groups
        """
        try:
            if not hasattr(self.message, 'term'):
                return
            await self.message.term()
            md = self.message.metadata()
            stream_seq = md.sequence.stream
            request = {
                "station_name": self.internal_station_name,
                "error": reason,
                "partition": self.partition,
                "cg_name": self.cg_name,
                "seq": stream_seq,
            }
            await self.connection.broker_manager.publish(
                "$memphis_nacked_dls", json.dumps(request).encode("utf-8")
            )
        except Exception as e:
            raise MemphisConnectError(str(e)) from e

    def get_data(self):
        """Receive the message."""
        try:
            return bytearray(self.message.data)
        except Exception:
            return

    async def get_data_deserialized(self):
        """Receive the message."""
        try:
            if self.connection.schema_updates_data and self.connection.schema_updates_data[self.internal_station_name] != {}:
                schema_type = self.connection.schema_updates_data[
                    self.internal_station_name
                ]["type"]
                try:
                    await self.station.validate_msg(bytearray(self.message.data))
                except Exception as e:
                    raise MemphisSchemaError("Deserialization has been failed since the message format does not align with the currently attached schema: " + str(e))
                if schema_type == "protobuf":
                    proto_msg = self.connection.proto_msgs[self.internal_station_name]
                    proto_msg.ParseFromString(self.message.data)
                    return proto_msg
                if schema_type == "avro":
                    return json.loads(bytearray(self.message.data))
                if schema_type == "json":
                    return json.loads(bytearray(self.message.data))
                if schema_type == "graphql":
                    message = bytearray(self.message.data)
                    decoded_str = message.decode("utf-8")
                    return decoded_str
            else:
                return bytearray(self.message.data)
        except Exception:
            return

    def get_headers(self):
        """Receive the headers."""
        try:
            return self.message.headers
        except Exception:
            return

    def get_sequence_number(self):
        """Get message sequence number."""
        try:
            return self.message.metadata.sequence.stream
        except Exception:
            return

    def get_timesent(self):
        """Get timestamp when the message was sent."""
        try:
            md = self.message.metadata()
            return md.timestamp
        except Exception:
            return

    async def delay(self, delay):
        """Delay and resend the message after delay seconds"""
        if (
            "$memphis_pm_id" in self.message.headers
            and "$memphis_pm_cg_name" in self.message.headers
        ):
            raise MemphisError("cannot delay DLS message")
        try:
            await self.message.nak(delay=delay)
        except Exception as e:
            raise MemphisError(str(e)) from e
