import asyncio

from memphis import Memphis


async def main():
    try:
        memphis = Memphis()
        await memphis.connect(host="localhost", username="root", connection_token="memphis")

        producer = memphis.producer(
            station_name="<station-name>", producer_name="<producer-name>")
        for i in range(100):
            await producer.produce(bytearray('Message #'+str(i)+': Hello world', 'utf-8'))

    except Exception as e:
        print(e)

    finally:
        await memphis.close()

if __name__ == '__main__':
    asyncio.run(main())
