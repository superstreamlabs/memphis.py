from __future__ import annotations
import asyncio

from memphis import Memphis, MemphisConnectError, MemphisError, MemphisHeaderError


async def main():
    async def msg_handler(msgs, error, context):
        try:
            for msg in msgs:
                print("message: ", msg.get_data())
                await msg.ack()
                headers = msg.get_headers()
            if error:
                print(error)
        except (MemphisError, MemphisConnectError, MemphisHeaderError) as e:
            print(e)
            return

    try:
        memphis = Memphis()
        await memphis.connect(
            host="<memphis-host>",
            username="<application type username>",
            connection_token="<broker-token>",
        )

        consumer = await memphis.consumer(
            station_name="<station-name>",
            consumer_name="<consumer-name>",
            consumer_group="",
        )

        consumer.set_context({"key": "value"})
        consumer.consume(msg_handler)
        # Keep your main thread alive so the consumer will keep receiving data
        await asyncio.Event().wait()

    except (MemphisError, MemphisConnectError) as e:
        print(e)

    finally:
        await memphis.close()


if __name__ == "__main__":
    asyncio.run(main())
