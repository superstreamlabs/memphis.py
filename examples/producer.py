import asyncio
from memphis import Memphis


async def main():
    try:
        memphis = Memphis()
        await memphis.connect(host="localhost", username="root", connection_token="memphis")
        await memphis.produce(station_name="<station-name>", producer_name="<producer-name>", message=bytearray('Message #: Hello world', 'utf-8'), ack_wait_sec=5)

    except Exception as e:
        print(e)

    finally:
        await memphis.close()

if __name__ == '__main__':
    asyncio.run(main())
