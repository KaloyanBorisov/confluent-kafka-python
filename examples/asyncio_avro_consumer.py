#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2025 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# A minimal example demonstrating AsyncIO Avro consumer with Schema Registry.
# Pairs with asyncio_avro_producer.py — run the producer first to populate the topic.

import argparse
import asyncio
import signal

from confluent_kafka.aio import AIOConsumer
from confluent_kafka.schema_registry import AsyncSchemaRegistryClient
from confluent_kafka.schema_registry._async.avro import AsyncAvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

running = True


def signal_handler(*_):
    global running
    running = False


async def main(args):
    sr_conf = {'url': args.schema_registry}
    if args.sr_api_key and args.sr_api_secret:
        sr_conf['basic.auth.user.info'] = f"{args.sr_api_key}:{args.sr_api_secret}"
    sr_client = AsyncSchemaRegistryClient(sr_conf)

    # AsyncAvroDeserializer fetches the schema from Schema Registry automatically
    # using the schema ID embedded in each message — no need to specify the schema here
    avro_deserializer = await AsyncAvroDeserializer(sr_client)

    consumer = AIOConsumer({
        'bootstrap.servers': args.bootstrap_servers,
        'group.id': args.group,
        'auto.offset.reset': 'earliest',
    })

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    await consumer.subscribe([args.topic])

    try:
        while running:
            # AsyncIO Pattern: Non-blocking poll — yields to the event loop while waiting
            msg = await consumer.poll(1.0)
            if msg is None:
                continue

            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            # AsyncIO Pattern: Async deserialization — schema lookup is a non-blocking await
            user = await avro_deserializer(
                msg.value(),
                SerializationContext(msg.topic(), MessageField.VALUE)
            )

            if user is not None:
                print(f"Consumed from {msg.topic()} [{msg.partition()}] @ {msg.offset()}: {user}")

    finally:
        # AsyncIO Pattern: Proper async cleanup
        await consumer.unsubscribe()
        await consumer.close()
        print("Consumer closed.")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='AsyncIO Avro consumer example')
    parser.add_argument('-b', dest='bootstrap_servers', required=True, help='Bootstrap broker(s) (host[:port])')
    parser.add_argument('-s', dest='schema_registry', required=True, help='Schema Registry (http(s)://host[:port])')
    parser.add_argument('--sr-api-key', dest='sr_api_key', default=None, help='Confluent Cloud SR API key (optional)')
    parser.add_argument(
        '--sr-api-secret', dest='sr_api_secret', default=None, help='Confluent Cloud SR API secret (optional)'
    )
    parser.add_argument('-t', dest='topic', default='example_asyncio_avro', help='Topic name')
    parser.add_argument('-g', dest='group', default='example_asyncio_avro', help='Consumer group')
    args = parser.parse_args()

    asyncio.run(main(args))
