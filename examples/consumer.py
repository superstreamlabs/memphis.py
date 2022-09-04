import asyncio
from memphis import Memphis


async def main():
    async def msg_handler(msgs, error):
        try:
            for msg in msgs:
                print("message: ", msg.get_data())
                await msg.ack()
            if error:
                print(error)
        except Exception as e:
            print(e)
            return

    try:
        memphis = Memphis()
        await memphis.connect(host="<memphis-host>", username="<application type username>", connection_token="<broker-token>")

        consumer = await memphis.consumer(
            station_name="<station-name>", consumer_name="<consumer-name>", consumer_group="")
        consumer.consume(msg_handler)
        # Keep your main thread alive so the consumer will keep receiving data
        await asyncio.sleep(5)

    except Exception as e:
        print(e)

    finally:
        await memphis.close()

if __name__ == '__main__':
    asyncio.run(main())
