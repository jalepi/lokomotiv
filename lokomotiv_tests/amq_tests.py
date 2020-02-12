from typing import List

import asyncio
import unittest
import multiprocessing

from lokomotiv.reqrep import run_broker, recvsend, send

def message_handler(message: List[bytes]) -> List[bytes]:
    #return [msg_part[::-1] for msg_part in message]
    return message

def server_process(connect: str):
    return recvsend(connect, message_handler)

given_messages = [
    [b'hello', b'world'],
    [b'jar', b'of', b'sweets'],
    [b'sfhasudfasdfuasdf'] * 500,
] * 2000

expected_messages = [message_handler(given_message) for given_message in given_messages]

class AmqTest(unittest.TestCase):
    def test_simple_broker(self):
        broker_process = multiprocessing.Process(target=run_broker, kwargs={'frontend': 'tcp://*:5559', 'backend': 'tcp://*:5560'})
        worker_process = multiprocessing.Process(target=server_process, kwargs={'connect': 'tcp://localhost:5560'})

        try:
            broker_process.start()
            worker_process.start()

            actual_messages = send(connect='tcp://localhost:5559', message_list=given_messages)
            
            self.assertEqual([am for am in actual_messages], [em for em in expected_messages])
        
        finally:
            broker_process.terminate()
            worker_process.terminate()
