"""
An example consumer for the Memphis.dev python SDK.
"""

import asyncio
import os
import json
from memphis import Memphis, MemphisConnectError, MemphisError
from memphis.message import Message


async def main():
    """
    Async main function used for the asyncio runtime.
    """
    try:
        # Connecting to the broker
        memphis = Memphis()

        await memphis.connect(
            host="<memphis-host>",
            username="<memphis-username>",
            password="<memphis-password>",
            account_id=<memphis-accountId>,  # For cloud users on, at the top of the overview page
        )

        consumer = await memphis.consumer(
            station_name="<station-name>",
            consumer_name="<consumer-name>",
        )

        while True:
            messages: list[
                Message
            ] = await consumer.fetch()  # Type-hint the return here for LSP integration

            if len(messages) == 0:
                continue

            for consumed_message in messages:
                msg_data = json.loads(consumed_message.get_data())

                # Do something with the message data
                print(msg_data)
                await consumed_message.ack()

    except Exception as e:
        print(e)
    finally:
        if memphis != None:
            await memphis.close()


if __name__ == "__main__":
    asyncio.run(main())
