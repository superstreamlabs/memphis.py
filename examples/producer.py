from __future__ import annotations

import asyncio

from memphis import (
    Headers,
    Memphis,
    MemphisConnectError,
    MemphisError,
    MemphisHeaderError,
    MemphisSchemaError,
)


async def main():
    try:
        memphis = Memphis()
        await memphis.connect(
            host="<memphis-host>",
            username="<application type username>",
            connection_token="<broker-token>",
        )

        producer = await memphis.producer(
            station_name="<station-name>", producer_name="<producer-name>"
        )
        headers = Headers()
        headers.add("key", "value")
        for i in range(5):
            await producer.produce(
                bytearray("Message #" + str(i) + ": Hello world", "utf-8"),
                headers=headers,
            )  # you can send the message parameter as dict as well

    except (
        MemphisError,
        MemphisConnectError,
        MemphisHeaderError,
        MemphisSchemaError,
    ) as e:
        print(e)

    finally:
        await memphis.close()


if __name__ == "__main__":
    asyncio.run(main())
