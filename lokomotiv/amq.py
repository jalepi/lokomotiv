import asyncio
import zmq.asyncio
from typing import List, AsyncIterable, TypeVar, Callable, Awaitable


async def subscribe(sub: str, topic: bytes, bind: bool=False) -> AsyncIterable[List[bytes]]:
    with zmq.asyncio.Context() as context:
        with context.socket(zmq.SUB) as subscriber:
            if bind:
                subscriber.bind(sub)
            else:
                subscriber.connect(sub)
            subscriber.subscribe(topic)
            while True:
                yield await subscriber.recv_multipart()

            
async def publish(pub: str, messages: AsyncIterable[List[bytes]], bind: bool=False) -> AsyncIterable:
    with zmq.asyncio.Context() as context:
        with context.socket(zmq.PUB) as publisher:
            if bind:
                publisher.bind(pub)
            else:
                publisher.connect(pub)
            async for message in messages:
                yield await publisher.send_multipart(message)


T = TypeVar('T')
R = TypeVar('R')

async def middleware(loop: AsyncIterable[T], fn: Callable[[T], Awaitable[R]]) -> AsyncIterable[R]:
    async for elem in loop:
        yield await fn(elem)


import pickle
import importlib


async def add_topic(messages: AsyncIterable[List[bytes]], topic: bytes) -> AsyncIterable[List[bytes]]:
    async for message in messages:
        yield [topic, *message]

async def remove_topic(messages: AsyncIterable[List[bytes]]) -> AsyncIterable[List[bytes]]:
    async for message in messages:
        yield message[1:]

async def work(messages: AsyncIterable[List[bytes]]) -> AsyncIterable[List[bytes]]:
    async for message in messages:
        [topic, id, *message] = message

        if topic == b'run':
            try:
                result = worker_run(message)
                yield [b'result', id, result]

            except Exception as ex:
                print(ex)
                yield [b'exception', id, pickle.dumps(ex)]

def worker_run(message: List[bytes]) -> bytes:
    print('message')
    print(message)

    [module_bytes, function_bytes, *params_bytes] = message
    
    print('params')
    print(params_bytes)

    module_name = module_bytes.decode('utf8')
    function_name = function_bytes.decode('utf8')

    print(f'{module_name}.{function_name}')

    module = importlib.import_module(module_name)
    function = module.__dict__[function_name]

    print(module)
    print(function)

    params = [pickle.loads(param_bytes) for param_bytes in params_bytes]
    print(params)

    result = function(*params)
    return pickle.dumps(result)


