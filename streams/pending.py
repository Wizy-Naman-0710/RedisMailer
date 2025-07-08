import redis.asyncio as redis
import asyncio
import redis.exceptions
from consumer import send_email

STREAM_KEY = "mystream"
CONSUMER_GROUP = "email_group"
CONSUMER_NAME = "email_reclaimer"
IDLE_TIME_MS = 5 * 60 * 1000
FETCH_COUNT = 10

async def process_claimed_message(r, message_id, data):
    try:
        to = data[b'email'].decode()
        subject = data[b'subject'].decode()
        body = data[b'body'].decode()

        print(f"[PROCESS] Sending reclaimed email to {to}")
        await send_email(to, subject, body)

        await r.xack(STREAM_KEY, CONSUMER_GROUP, message_id)
        print(f"[ACK] Acknowledged {message_id}")
    except KeyError as e:
        print(f"[ERROR] Malformed data for message {message_id}: missing key {e}")
    except Exception as e:
        print(f"[ERROR] Failed to process {message_id}: {e}")

async def check_pending_entries():
    r = redis.asyncio.Redis()
    try:
        pending_summary = await r.xpending(STREAM_KEY, CONSUMER_GROUP)
        if pending_summary['pending'] == 0:
            print("No pending messages to reclaim.")
            return

        pending_entries = await r.xpending_range(
            STREAM_KEY,
            CONSUMER_GROUP,
            min='-',
            max='+',
            count=FETCH_COUNT
        )

        if not pending_entries:
            print("No messages found to reclaim")
            return

        message_ids_to_claim = [entry['message_id'] for entry in pending_entries]

        claimed_messages = await r.xclaim(
            STREAM_KEY,
            CONSUMER_GROUP,
            CONSUMER_NAME,
            min_idle_time=IDLE_TIME_MS,
            message_ids=message_ids_to_claim
        )

        if not claimed_messages:
            print("There are no messages to be claimed")
            return

        print(f"[CLAIM] {len(claimed_messages)} Messages are pending")

        tasks = [
            process_claimed_message(r, msg_id, data)
            for msg_id, data in claimed_messages
        ]
        await asyncio.gather(*tasks)

    except redis.exceptions.ResponseError as e:
        if "NOGROUP" in str(e):
            print(f"[ERROR] Consumer group '{CONSUMER_GROUP}' does not exist.")
        else:
            print(f"[ERROR] Redis error: {e}")
    except Exception as e:
        print(f"[ERROR] An unexpected error occurred: {e}")
    finally:
        await r.aclose()
        print("Redis connection closed.")

if __name__ == "__main__":
    asyncio.run(check_pending_entries())