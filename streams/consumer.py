import asyncio
import redis.asyncio as redis
import asyncio
from aiosmtplib import SMTP, SMTPException
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

STREAM_KEY = "mystream"
CONSUMER_GROUP = "email_group"
CONSUMER_NAME = "email_worker"
SMTP_HOST = "smtp.gmail.com"
SMTP_PORT = 587
EMAIL_USERNAME = "murarkanaman@gmail.com"
EMAIL_PASSWORD = "vilgsuiueyaqrcqy"  

def read_html_file(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            html_content = file.read()
        return html_content
    
    except FileNotFoundError:
        print(f"Error: The file '{file_path}' was not found.")
        return None
    
    except Exception as e:
        print(f"An error occurred: {e}")
        return None
    

async def send_email(to_email: str, subject: str, body: str):
    msg = MIMEMultipart("alternative")
    msg["From"] = EMAIL_USERNAME
    msg["To"] = to_email
    msg["Subject"] = subject

        
    plain_text_message = MIMEText("Sent via aiosmtplib", "plain", "utf-8")
    html_message = MIMEText(read_html_file("index.html"), "html", "utf-8")
        
    msg.attach(plain_text_message)
    msg.attach(html_message)

    try:
        smtp = SMTP(hostname=SMTP_HOST, port=SMTP_PORT, start_tls=True)

        await smtp.connect()
        await smtp.login(EMAIL_USERNAME, EMAIL_PASSWORD)

        await smtp.send_message(msg)
        print(f"[SUCCESS] Email sent to {to_email}")

        await smtp.quit()
    except SMTPException as e:
        print(f"[ERROR] SMTP error occurred: {e}")
    except Exception as e:
        print(f"[ERROR] Unexpected error: {e}")


async def ensure_group_exists(r):
    try:
        await r.xgroup_create(STREAM_KEY, CONSUMER_GROUP, id="0", mkstream=True)
        print("Consumer group created")
    except redis.ResponseError as e:
        if "BUSYGROUP" in str(e):
            print("[ERROR] Consumer group already exists")
        else:
            raise


async def process_message(message_id, data, r, stream_key, consumer_group):
    if data is None:
        print(f"[SKIP] Skipping empty message {message_id}")
        return

    try:
        decoded = {k.decode(): v.decode() for k, v in data.items()}
        await send_email(
            decoded['email'],
            decoded['subject'],
            decoded['body']
        )
        await r.xack(stream_key, consumer_group, message_id)
        print(f"[ACK] Acknowledged {message_id}")
    except Exception as e:
        print(f"[ERROR] Failed to process message {message_id}: {e}")


async def main():
    r = redis.Redis()
    await ensure_group_exists(r)

    print("[START] Starting email consumer...")

    while True:
        try:
            response = await r.xreadgroup(
                groupname=CONSUMER_GROUP,
                consumername=CONSUMER_NAME,
                streams={STREAM_KEY: '>'},
                count=10,
                block=5000,
            )
           
            if response:
                tasks = []
                for stream_key, messages in response:
                    for message_id, data in messages:
                        task = asyncio.create_task(
                            process_message(message_id, data, r, stream_key, CONSUMER_GROUP)
                        )
                        tasks.append(task)

                if tasks:
                    await asyncio.gather(*tasks)

        except Exception as e:
            print(f"[ERROR] {e}")

if __name__ == "__main__":
    asyncio.run(main())

