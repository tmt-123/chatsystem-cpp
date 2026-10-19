import json
from pathlib import Path
import time
import pika
credential = json.loads(Path("/bench/private/credentials.json").read_text())
connection = pika.BlockingConnection(pika.ConnectionParameters("soarim-bench-rabbitmq",
    credentials=pika.PlainCredentials("soarim",credential["mq"]),socket_timeout=3))
queue = connection.channel().queue_declare("msg_queue",passive=True).method
print(json.dumps({"timestamp":time.time(),"ready":queue.message_count,"consumers":queue.consumer_count}))
connection.close()
