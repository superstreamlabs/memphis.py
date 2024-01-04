"""
An example producer for the Memphis.dev python SDK.
"""

import asyncio
import os
from memphis import Memphis, MemphisConnectError, MemphisError


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
            account_id= <memphis-accountId>,  # For cloud users on, at the top of the overview page
        )

        # Creating a producer and producing a message.
        # You can also use the memphis.producer function
        producer = await memphis.producer(
            station_name="<station-name>",  # Matches the station name in memphis cloud
            producer_name="<producer-name>",
        )

        for i in range(10):
            await producer.produce(message={"id": i, "chocolates_to_eat": 3})

    except (MemphisError, MemphisConnectError) as e:
        print(e)
    finally:
        await memphis.close()


if __name__ == "__main__":
    asyncio.run(main())
