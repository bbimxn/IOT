#รันบน vscode ใช้กับ raspi เป็นกำหนดค่าความสว่างไฟในนี้แล้วส่งไป ras
import asyncio
from aiocoap import Context, Message
import aiocoap


SERVER_IP = "10.30.4.78"
SERVER_PORT = 5683


async def get_temperature():

    protocol = await Context.create_client_context()

    request = Message(
        code=aiocoap.Code.GET,
        uri=f"coap://{SERVER_IP}:{SERVER_PORT}/temperature"
    )

    response = await protocol.request(request).response

    print("--------------------------")
    print("Response:")
    print("Code    :", response.code)
    print("Payload :", response.payload.decode())


async def led_on():

    protocol = await Context.create_client_context()

    request = Message(
        code=aiocoap.Code.PUT,
        uri=f"coap://{SERVER_IP}:{SERVER_PORT}/led",
        payload=b"ON"    #ส่งเป็น byte
    )

    response = await protocol.request(request).response

    print("--------------------------")
    print("Response:")
    print("Code    :", response.code)
    print("Payload :", response.payload.decode())


async def led_off():

    protocol = await Context.create_client_context()

    request = Message(
        code=aiocoap.Code.PUT,
        uri=f"coap://{SERVER_IP}:{SERVER_PORT}/led",
        payload=b"OFF"  #ส่งเป็น byte
    )

    response = await protocol.request(request).response

    print("--------------------------")
    print("Response:")
    print("Code    :", response.code)
    print("Payload :", response.payload.decode())

async def pwm():

    protocol = await Context.create_client_context()

    request = Message(
        code=aiocoap.Code.PUT,
        uri=f"coap://{SERVER_IP}:{SERVER_PORT}/pwm",
        payload = b"100"
    )

    response = await protocol.request(request).response

    print("--------------------------")
    print("Response:")
    print("Code    :", response.code)
    print("Payload :", response.payload.decode())


async def main():   #สร้างฟังก์ชันชื่อ main() แบบ Asynchronous

    # await get_temperature()   #await หมายถึง รอให้คำสั่งนี้ทำงานเสร็จก่อน แล้วจึงไปคำสั่งถัดไป

    # await led_on()

    await pwm()

    # await asyncio.sleep(2)

    # await led_off()


asyncio.run(main())   #เริ่มต้นการทำงานของ main()
