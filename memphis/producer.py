import asyncio

from memphis import Memphis

#TODO need to move this example to examples directory - need to fix the problems with imports
async def main():
    try:
        memphis = Memphis()
        await memphis.connect(host="localhost", username="root", connection_token="memphis")

        # producer = memphis.producer(
        #     station_name="sname", producer_name="pname")
        # for i in range(100):
        #     await producer.produce(bytearray('Message #'+str(i)+': Hello world', 'utf-8'))

    except Exception as e:
        print("error" ,e)

    # finally:
    #     await memphis.close()


if __name__ == '__main__':
    asyncio.run(main())
