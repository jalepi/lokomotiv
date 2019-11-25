import argparse
import asyncio
from typing import Iterable, List, AsyncIterable
import inspect
import pickle

import lokomotiv.amq
import lokomotiv.worker


async def input_loop_async() -> AsyncIterable[List[bytes]]:
    while True:
        msg_parts : List[bytes] = []
        while True:
            msg = input().encode('utf8')
            if (len(msg) == 0):
                yield msg_parts
                msg_parts = []
            else:
                msg_parts.append(msg)


async def start(task: str, pub: str, sub: str, topic: str) -> AsyncIterable:
    if task == 'broker':

        async def log_middleware(something):
            print(something)
            return something

        messages = lokomotiv.amq.subscribe(sub=sub, topic=topic, bind=True)
        messages = lokomotiv.amq.middleware(messages, log_middleware)
        signals = lokomotiv.amq.publish(pub=pub, messages=messages, bind=True)
        
        async for signal in signals:
            yield

    elif task == 'publisher':
        async for _ in lokomotiv.amq.publish(pub=pub, messages=input_loop_async()):
            yield

    elif task == 'subscriber':
        async for message in lokomotiv.amq.subscribe(sub=sub, topic=topic):
            print(message)
            yield

    elif task == 'worker':
        messages = lokomotiv.amq.subscribe(sub=sub, topic=topic)
        messages = lokomotiv.amq.remove_topic(messages)
        messages = lokomotiv.amq.work(messages)
        messages = lokomotiv.amq.add_topic(messages, topic)

        async for _ in lokomotiv.amq.publish(pub=pub, messages=messages):
            yield _

    elif task == 'work-publisher':

        async def pack_work_messages(messages):
            async for message in messages:
                print(message)
                [topic, action, *params] = message

                if action == b'run':
                    [id, module, function, *params] = params
                    yield [topic, action, id, module, function, *[pickle.dumps(param.decode('utf8')) for param in params]]

                elif action == b'result':
                    [id, result, *_] = params
                    yield [topic, action, id, pickle.dumps(result)]

                elif action == b'exception':
                    [id, exception, *_] = params
                    yield [topic, action, id, pickle.dumps(param)]


        messages = input_loop_async()
        messages = pack_work_messages(messages)
        async for _ in lokomotiv.amq.publish(pub=pub, messages=messages):
            yield _

    elif task == 'work-subscriber':
        async def unpack_work_messages(messages):
            async for message in messages:
                print(message)
                [topic, action, *params] = message

                if action == b'run':
                    [id, module, function, *params] = params

                elif action == b'result':
                    [id, result, *_] = params
                    yield [topic, action, id, pickle.loads(result)]

                elif action == b'exception':
                    [id, exception, *_] = params
                    yield [topic, action, id, pickle.loads(param)]

        messages = lokomotiv.amq.subscribe(sub=sub, topic=topic)
        messages = unpack_work_messages(messages)

        async for message in messages:
            print(message)
            yield

if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument('--task', 
                        help='Start a task',
                        type=str,
                        choices=['broker', 'publisher', 'subscriber', 'worker', 'work-publisher', 'work-subscriber'],
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

    task = args.task
    pub = args.pub
    sub = args.sub
    topic = args.topic.encode('utf8')

    async def run_start_forever():
        async for _ in start(task, pub, sub, topic):
            pass

    asyncio.run(run_start_forever())