#!/usr/bin/env python3
import pika
import json


def main():
    """
    main
    """
    connection = pika.BlockingConnection(
        pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    channel.queue_declare(queue='hello')

    def callback(ch, method, properties, body):
        messages = json.loads(body)
        print(f"[x] Received {messages}")

    channel.basic_consume(queue='task_queue', auto_ack=True,
                          on_message_callback=callback)

    print('[*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("Interrupter")
        exit(0)
