import asyncio
import logging
import os
import signal
from typing import Dict, Any

import redis.asyncio as redis
import redis.asyncio
import redis.exceptions
from aiosmtplib import SMTP, SMTPException
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# --- Configuration ---
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
SMTP_HOST = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", 587))

EMAIL_USERNAME = os.getenv("EMAIL_USERNAME", "murarkanaman@gmail.com")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD", "vilgsuiueyaqrcqy")

STREAM_KEY = "email_stream"
CONSUMER_GROUP = "email_processing_group"
CONSUMER_NAME = f"consumer-{os.uname().nodename}"

# --- Stream Processing Configuration ---
# How long a message can be pending before the reclaimer picks it up (in milliseconds)
RECLAIM_IDLE_TIME_MS = 60000
# How often the reclaimer task runs to check for stuck messages (in seconds)
RECLAIM_INTERVAL_SECONDS = 30
# How many messages to fetch per read/claim operation
FETCH_COUNT = 10

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

def read_html_file(file_path: str) -> str:
    """Reads an HTML file and returns its content."""
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            return file.read()
    except FileNotFoundError:
        logger.critical(f"HTML template file not found at '{file_path}'. Exiting.")
        exit(1)
    except Exception as e:
        logger.critical(f"Failed to read HTML template file: {e}")
        exit(1)


async def ensure_group_exists(redis_client: redis.Redis):
    """Creates the consumer group on the stream if it doesn't already exist."""
    try:
        await redis_client.xgroup_create(STREAM_KEY, CONSUMER_GROUP, id="0", mkstream=True)
        logger.info(f"Consumer group '{CONSUMER_GROUP}' created for stream '{STREAM_KEY}'.")
    except redis.exceptions.ResponseError as e:
        if "BUSYGROUP" in str(e):
            logger.info(f"Consumer group '{CONSUMER_GROUP}' already exists.")
        else:
            logger.error(f"Failed to create consumer group: {e}")
            raise


async def send_email(redis_client: redis.Redis, message_id: str, to_email: str, subject: str, body: str, html_template: str):
    """Constructs and sends an email using aiosmtplib."""
    msg = MIMEMultipart("alternative")
    msg["From"] = EMAIL_USERNAME
    msg["To"] = to_email
    msg["Subject"] = subject

    plain_text_content = f"This is a notification about: {subject}. \n\n {body}"
    msg.attach(MIMEText(plain_text_content, "plain", "utf-8"))
    msg.attach(MIMEText(html_template.format(subject=subject, body=body), "html", "utf-8"))

    try:
        async with SMTP(hostname=SMTP_HOST, port=SMTP_PORT, start_tls=True) as smtp:
            await smtp.login(EMAIL_USERNAME, EMAIL_PASSWORD)
            await smtp.send_message(msg)
            logger.info(f"Successfully sent email to {to_email} with subject '{subject}'.")
    except SMTPException as e:
        logger.error(f"SMTP error while sending to {to_email}: {e.code} {e.message}")
        raise
    except Exception as e:
        logger.error(f"An unexpected error occurred during email sending: {e}")
        raise


async def process_message(redis_client: redis.Redis, message_id: str, data: Dict[str, Any], html_template: str):
    """Handles the business logic for a single message."""
    if not data:
        logger.warning(f"Skipping empty message data for message ID {message_id}.")
        return

    try:
        to_email = data.get('email')
        subject = data.get('subject')
        body = data.get('body')

        if not all([to_email, subject, body]):
            logger.error(f"Malformed data for message {message_id}. Data: {data}")
            await redis_client.xack(STREAM_KEY, CONSUMER_GROUP, message_id)
            return

        logger.info(f"Processing message {message_id} for {to_email}.")
        await send_email(redis_client, message_id, to_email, subject, body, html_template)
        await redis_client.xack(STREAM_KEY, CONSUMER_GROUP, message_id)
        logger.info(f"[ACK] Acknowledged message {message_id}.")

    except Exception as e:
        logger.error(f"Failed to process message {message_id}. It will be retried later. Error: {e}")


async def run_consumer(redis_client: redis.Redis, html_template: str, shutdown_event: asyncio.Event):
    """The main loop for consuming new messages from the stream."""
    logger.info(f"Starting consumer '{CONSUMER_NAME}'...")
    while not shutdown_event.is_set():
        try:
            response = await redis_client.xreadgroup(
                groupname=CONSUMER_GROUP,
                consumername=CONSUMER_NAME,
                streams={STREAM_KEY: '>'},
                count=FETCH_COUNT,
                block=5000,
            )

            if not response:
                continue

            tasks = []
            for _, messages in response:
                for message_id, data in messages:
                    tasks.append(process_message(redis_client, message_id, data, html_template))
            
            if tasks:
                await asyncio.gather(*tasks)

        except Exception as e:
            if not shutdown_event.is_set():
                logger.error(f"Error in consumer loop: {e}")
                await asyncio.sleep(5)


async def run_reclaimer(redis_client: redis.Redis, html_template: str, shutdown_event: asyncio.Event):
    """A background task that periodically checks for and reclaims idle messages."""
    logger.info("Starting reclaimer task...")
    while not shutdown_event.is_set():
        try:
            pending_summary = await redis_client.xpending(STREAM_KEY, CONSUMER_GROUP)
            if pending_summary['pending'] > 0:
                logger.info(f"Reclaimer: Found {pending_summary['pending']} pending messages. Checking for idle ones.")
                pending_entries = await redis_client.xpending_range(
                    STREAM_KEY, CONSUMER_GROUP, min='-', max='+', count=FETCH_COUNT
                )
                
                if pending_entries:
                    message_ids_to_claim = [entry['message_id'] for entry in pending_entries]
                    claimed_messages = await redis_client.xclaim(
                        STREAM_KEY, CONSUMER_GROUP, CONSUMER_NAME,
                        min_idle_time=RECLAIM_IDLE_TIME_MS,
                        message_ids=message_ids_to_claim,
                    )
                    if claimed_messages:
                        logger.warning(f"[RECLAIM] Claimed {len(claimed_messages)} idle messages. Reprocessing...")
                        tasks = [process_message(redis_client, msg_id, data, html_template) for msg_id, data in claimed_messages]
                        await asyncio.gather(*tasks)

        except redis.exceptions.ResponseError as e:
            if "NOGROUP" in str(e):
                logger.error(f"Reclaimer: Consumer group '{CONSUMER_GROUP}' does not exist. It will be created shortly.")
            else:
                logger.error(f"Reclaimer: Redis error: {e}")
        except Exception as e:
            if not shutdown_event.is_set():
                logger.error(f"Reclaimer: An unexpected error occurred: {e}")
        
        try:
            await asyncio.wait_for(shutdown_event.wait(), timeout=RECLAIM_INTERVAL_SECONDS)
        except asyncio.TimeoutError:
            pass


async def main():
    """Main entry point for the application."""
    redis_client = None
    shutdown_event = asyncio.Event()
    html_template = read_html_file("index.html")
    
    loop = asyncio.get_running_loop()

    def handle_shutdown_signal(sig: signal.Signals):
        """Sets the shutdown event when a signal is received."""
        logger.info(f"Received exit signal {sig.name}, shutting down gracefully...")
        shutdown_event.set()

    # Add signal handlers for SIGINT (Ctrl+C) and SIGTERM
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, handle_shutdown_signal, sig)

    try:
        logger.info(f"Connecting to Redis at {REDIS_HOST}:{REDIS_PORT}...")
        redis_client = redis.asyncio.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        await redis_client.ping()
        logger.info("Redis connection successful.")
        await ensure_group_exists(redis_client)

        consumer_task = asyncio.create_task(run_consumer(redis_client, html_template, shutdown_event))
        reclaimer_task = asyncio.create_task(run_reclaimer(redis_client, html_template, shutdown_event))

        await asyncio.gather(consumer_task, reclaimer_task)

    except redis.exceptions.ConnectionError as e:
        logger.critical(f"Failed to connect to Redis: {e}")
    except asyncio.CancelledError:
        logger.info("Application tasks cancelled during shutdown.")
    finally:
        logger.info("Shutdown process initiated...")
        if redis_client:
            await redis_client.aclose()
            logger.info("Redis connection closed.")
        logger.info("Shutdown complete.")


if __name__ == "__main__":
    asyncio.run(main())