#!/usr/bin/env python3
import pika
import json

# connect to channel
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

# declare a queue
channel.queue_declare(queue='task_queue', durable=True)

message = {
    'id': 1,
    'name': 'name1'
}

for _ in range(10):
    channel.basic_publish(exchange='',
                          routing_key='task_queue',
                          body=json.dumps(message),
                          properties=pika.BasicProperties(delivery_mode=2))

print("Finish sending")
connection.close()
