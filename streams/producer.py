# producer.py
import asyncio
import redis.asyncio as redis

STREAM_KEY = "mystream"

async def main():
    r = redis.Redis()

    message = {
        "email": "f20241074@pilani.bits-pilani.ac.in",
        "subject": "This is the next Mail",
        "body": "Hello world."
    }
    message_id = await r.xadd(STREAM_KEY, message)
    print(f"Sent message {message_id}: {message}")

    await r.aclose()

if __name__ == "__main__":
    asyncio.run(main())

