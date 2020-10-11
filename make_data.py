#!/usr/bin/env python
from datetime import datetime, timedelta
import random
import time

import kombu
from kombu.pools import producers


uids = [
    "foo",
    "bar",
    "baz",
    "abc",
    "def",
    "ghi",
    "jkl",
    "mno",
    "boogaloo",
]


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
                    "uid1": random.choice(uids),
                    "start_time": datetime.now() - timedelta(minutes=5),
                    "stop_time": datetime.now(),
                },
                exchange=exchange,
            )
            producer.publish(
                {
                    "uid1": random.choice(uids),
                    "start_time": datetime.now() - timedelta(minutes=5),
                    "stop_time": datetime.now(),
                },
                exchange=exchange,
            )
            time.sleep(0.5)
