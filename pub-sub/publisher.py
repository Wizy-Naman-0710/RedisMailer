import redis
import json

r = redis.Redis()

def publish_email(recipient, subject, body):
    message = {
        'to': recipient,
        'subject': subject,
        'body': body
    }
    r.publish('email:channel', json.dumps(message))
    print("Published email to:", recipient)

publish_email("email123@gmail.com", "Hello from Redis", "This mail is just for testing purposes.")
