import asyncio
from typing import Iterable, AsyncIterable, TypeVar, Callable, Awaitable

async def input_loop_async() -> AsyncIterable[str]:
    while True:
        yield input()

async def generate_loop_async() -> AsyncIterable[str]:
    i = 0
    while True:
        await asyncio.sleep(1)
        yield f'{i}'
        i = i + 1

async def process(message: str) -> str:
    await asyncio.sleep(1)
    return message.upper()

async def display(messages: AsyncIterable[str]):
    async for message in messages:
        print(message)

T = TypeVar('T')
R = TypeVar('R')

async def middleware(loop: AsyncIterable[T], fn: Callable[[T], Awaitable[R]]) -> AsyncIterable[R]:
    async for item in loop:
        yield await fn(item)

def main():
    event_loop = asyncio.get_event_loop()
    try:
        async def prepend(msgs : Iterable[str], amsgs : AsyncIterable[str]):
            for msg in msgs:
                yield msg
            async for amsg in amsgs:
                yield amsg

        messages = generate_loop_async()
        messages = prepend(['hi', 'there'], messages)

        messages = middleware(messages, process)

        event_loop.run_until_complete(display(messages))
    finally:
        event_loop.close()


if __name__ == '__main__':
    main()