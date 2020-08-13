#!/usr/bin/env python
import datetime
import kombu
from kombu.pools import producers


with kombu.Connection("amqp://guest:guest@localhost:5672") as conn:
    channel = conn.channel()
    exchange = kombu.Exchange("input_exchange", type="topic")
    exchange.declare(channel=channel)

    queue = kombu.Queue("test_queue", exchange=exchange, durable=True, message_ttl=3600)
    queue.declare(channel=channel)
    queue.queue_bind(channel=channel)
    with producers[conn].acquire(block=True) as producer:
        while True:
            producer.publish(
                {
                    "uid1": "foobar",
                    "start_time": datetime.datetime.now() - datetime.timedelta(minutes=5),
                    "stop_time": datetime.datetime.now(),
                },
                exchange=exchange,
            )
            producer.publish(
                {
                    "uid1": "foobar",
                    "start_time": datetime.datetime.now() - datetime.timedelta(minutes=5),
                    "stop_time": datetime.datetime.now(),
                },
                exchange=exchange,
            )
            break