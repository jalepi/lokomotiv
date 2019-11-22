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