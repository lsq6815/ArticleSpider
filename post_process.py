#!/usr/bin/env python3
import json

import pika
from pymongo import MongoClient

import settings

pages = []
Mongo_Collection = None

def callback(ch, method, properties, body):
    """
    处理消息的回调函数

    :param ch
    :param method
    :param properties
    :param body: 消息
    """
    page = json.loads(body)
    pages.append(page)
    print(f"Received page: {page['link']}")

    res = Mongo_Collection.insert_one(page)
    print(f"Document inserted with id: {res.inserted_id}")

    # 保存到本地
    if len(pages) >= settings.THRESHOLD:
        print("Dump all pages")
        while pages:
            p = pages.pop()
            filename = ''.join((ch if ch.isalnum() else '_')
                               for ch in p['link']) + 'html.json'
            # write file in json
            with open(filename, 'w', encoding='utf8') as f:
                json.dump(p, f, ensure_ascii=False)

            print(f"Store file {filename}")

if __name__ == '__main__':
    try:
        connection = pika.BlockingConnection(
            pika.ConnectionParameters('localhost'))
        channel = connection.channel()

        channel.queue_declare(queue=settings.QUEUE_NAME, durable=True)

        channel.basic_consume(queue=settings.QUEUE_NAME, auto_ack=True,
                              on_message_callback=callback)

        client = MongoClient(settings.MONGODB_HOSTNAME, settings.MONGODB_PORT)
        db = client[settings.MONGODB_DB]
        Mongo_Collection = db[settings.MONGODB_COLLECTION]

        print('[*] Waiting for messages. To exit press CTRL+C')
        channel.start_consuming()
    except KeyboardInterrupt:
        print("Interrupted")
        exit(0)
