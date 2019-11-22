import argparse
import asyncio
from typing import Iterable, List, AsyncIterable
import lokomotiv.amq
import lokomotiv.worker
import lokomotiv.mq

import inspect

if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument('--isasync', 
                        help='Use async implementation',
                        type=bool, 
                        default=False,
                        required=False)

    parser.add_argument('--task', 
                        help='Start a task',
                        type=str,
                        choices=['broker', 'publish', 'subscribe', 'work'],
                        required=True)

    parser.add_argument('--pub', 
                        help='Defines the publishing url to connect or bind. Example: tcp://*:5559',
                        type=str, 
                        default='tcp://localhost:5559',
                        required=False)

    parser.add_argument('--sub', 
                        help='Defines the subscribing url to connect or bind. Example: tcp://*:5560',
                        type=str, 
                        default='tcp://localhost:5560',
                        required=False)

    parser.add_argument('--topic', 
                        help='Defines the topic',
                        type=str, 
                        default='',
                        required=False)

    args = parser.parse_args()

    isasync = args.isasync 
    task = args.task
    pub = args.pub
    sub = args.sub
    topic = args.topic.encode('utf8')

    def input_loop() -> Iterable[List[bytes]]:
        while True:
            msg_parts : List[bytes] = []
            while True:
                msg = input().encode('utf8')
                if (len(msg) == 0):
                    yield msg_parts
                    msg_parts = []
                else:
                    msg_parts.append(msg)

    async def input_loop_async() -> AsyncIterable[List[bytes]]:
        while True:
            msg_parts : List[bytes] = []
            while True:
                msg = input().encode('utf8')
                if (len(msg) == 0):
                    yield [topic, *msg_parts]
                    msg_parts = []
                else:
                    msg_parts.append(msg)

    if isasync:
        if task == 'broker':

            async def log_middleware(something):
                print(something)
                return something

            messages = lokomotiv.amq.subscribe(sub=sub, topic=topic, bind=True)
            messages = lokomotiv.amq.middleware(messages, log_middleware)
            signals = lokomotiv.amq.publish(pub=pub, messages=messages, bind=True)
            
            async def async_broker_forever():
                async for signal in signals:
                    pass

            asyncio.run(async_broker_forever())

        elif task == 'publish':
            async def async_publish_forever():
                async for _ in lokomotiv.amq.publish(pub=pub, messages=input_loop_async()):
                    pass

            asyncio.run(async_publish_forever())

        elif task == 'subscribe':
            async def async_subscribe_forever():
                async for message in lokomotiv.amq.subscribe(sub=sub, topic=topic):
                    print(message)

            asyncio.run(async_subscribe_forever())

        elif task == 'work':
            for result in lokomotiv.worker.work(pub=pub, sub=sub, topic=topic):
                print(result)

    else:
        if task == 'broker':
            for _ in lokomotiv.mq.broker(pub=sub, sub=pub, topic=topic):
                pass

        elif task == 'publish':
            for _ in lokomotiv.mq.publish(pub=pub, topic=topic, messages=input_loop()):
                pass

        elif task == 'subscribe':
            for message in lokomotiv.mq.subscribe(sub=sub, topic=topic):
                print(message)

        elif task == 'work':
            for result in lokomotiv.worker.work(pub=pub, sub=sub, topic=topic):
                print(result)

