import asyncio
import json
import redis.asyncio as aioredis
from aiosmtplib import SMTP
from email.message import EmailMessage

async def send_email(to_email, subject, body):
    msg = EmailMessage()
    msg["From"] = "murarkanaman@gmail.com"
    msg["To"] = to_email
    msg["Subject"] = subject
    msg.set_content(body)

    smtp = SMTP(hostname="smtp.gmail.com", port=587)

    await smtp.connect()
    await smtp.login("murarkanaman@gmail.com", "vilgsuiueyaqrcqy")  
    await smtp.send_message(msg)
    await smtp.quit()

    print(f"Mail sent to {to_email}")

async def subscribe_and_send():
    r = aioredis.Redis()
    pubsub = r.pubsub()
    await pubsub.subscribe("email:channel")

    print("Subscribed to 'email:channel'...")

    async for message in pubsub.listen():
        if message["type"] == "message":
            data = json.loads(message["data"])
            await send_email(data["to"], data["subject"], data["body"])

if __name__ == "__main__":
    asyncio.run(subscribe_and_send())
