from __future__ import annotations

import json

from memphis.exceptions import MemphisConnectError, MemphisError


class Message:
    def __init__(self, message, connection, cg_name):
        self.message = message
        self.connection = connection
        self.cg_name = cg_name

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

    def get_data(self):
        """Receive the message."""
        try:
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
