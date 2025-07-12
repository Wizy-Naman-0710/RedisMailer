# producer.py
import asyncio
import redis.asyncio as redis
from redis.exceptions import RedisError
import os
import logging 

# --- Configuration ---
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
STREAM_KEY = os.getenv("STREAM_KEY", "email_stream")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

async def main(email: str, subject: str, body: str):
    """
    Establishes a Redis connection, sends a message, and then closes the connection.
    """
    redis_client = None
    try:
        # Establish a connection to the Redis server
        redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)
        await redis_client.ping()
        logger.info("Successfully connected to Redis at %s:%d", REDIS_HOST, REDIS_PORT)

        email_message = {
            "email": email,
            "subject": subject,
            "body": body
        }
        try:
            message_id = await redis_client.xadd(STREAM_KEY, email_message)
            logger.info("Message %s sent to stream '%s': %s", message_id, STREAM_KEY, email_message)
            return message_id
        except RedisError as e:
            logger.error("Failed to send message to stream '%s': %s", STREAM_KEY, e, exc_info=True)
            raise

    except RedisError:
        logger.critical("A Redis-related error occurred. The application will exit.")
    except Exception as e:
        logger.critical("An unexpected error occurred: %s", e, exc_info=True)
    finally:
        if redis_client:
            await redis_client.aclose()
            logger.info("Redis connection closed.")


def publish_mail(email: str, subject: str, body: str):
    asyncio.run(main(email, subject, body))

if __name__ == "__main__":
    publish_mail("f20241074@pilani.bits-pilani.ac.in", "Test Subject", "This is a test email body.")