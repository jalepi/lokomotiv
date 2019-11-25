import asyncio
import itertools
from typing import Iterable, AsyncIterable, TypeVar, Callable, Awaitable


async def input_loop_async() -> AsyncIterable[str]:
    while True:
        yield input()

async def generate_loop_async(prefix: str) -> AsyncIterable[str]:
    i = 0
    while True:
        await asyncio.sleep(1)
        yield f'{prefix}_{i}'
        i = i + 1

async def process(message: str) -> str:
    return message.upper()

async def display(messages: AsyncIterable[str]) -> None:
    async for message in messages:
        print(message)

T = TypeVar('T')
R = TypeVar('R')

async def middleware(loop: AsyncIterable[T], fn: Callable[[T], Awaitable[R]]) -> AsyncIterable[R]:
    async for item in loop:
        yield await fn(item)

async def main():
    try:
        
        first_messages = generate_loop_async('first')
        second_messages = generate_loop_async('second')


        while True:
            n = await first_messages.__anext__()
            print(n)

    except Exception as ex:
        print(ex)
        pass


if __name__ == '__main__':
    event_loop = asyncio.get_event_loop()
    event_loop.run_until_complete(main())