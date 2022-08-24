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
        await memphis.connect(host="localhost", username="root", connection_token="memphis")

        myfactory = await memphis.factory("shoham_shay_123567891012345")
        myStation = await memphis.station("station_shoham12345678911", myfactory.name)
        myProducer = await memphis.producer(myStation.name, "producername123")
        for i in range(1):
            await myProducer.produce(bytearray('Message #'+str(i)+': Hello world', 'utf-8'))
        myConsumer = await memphis.consumer(
        station_name=myStation.name, consumer_name="consumer_name", consumer_group="")
        myConsumer.consume(msg_handler)
        # Keep your main thread alive so the consumer will keep receiving data
        await asyncio.sleep(5)

        await myProducer.destroy()
        await myConsumer.destroy()
        await myStation.destroy()
        await myfactory.destroy()

    except Exception as e:
        print("error" ,e)

    # finally:
    #     await memphis.close()


if __name__ == '__main__':
    asyncio.run(main())
