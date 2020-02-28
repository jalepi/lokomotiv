import zmq
import multiprocessing
import threading
import time

def request(bind_to):
    with zmq.Context() as context:
        with context.socket(zmq.REQ) as socket:
            socket.bind(bind_to)
            while True:
                print(socket)
                socket.send_multipart([b'hello', b'world'])
                msg = socket.recv_multipart()
                print(f'REQ {msg}')
                time.sleep(1)

def reply(connect_to):
    with zmq.Context() as context:
        with context.socket(zmq.REP) as socket:
            socket.connect(connect_to)
            while True:
                msg = socket.recv_multipart()
                print(f'REP {msg}')
                socket.send_multipart(msg)


if __name__ == '__main__':
    print(__name__)
    # request_process = multiprocessing.Process(target=request, kwargs={'bind_to': 'tcp://*:9999'})
    # reply_process = multiprocessing.Process(target=reply, kwargs={'connect_to': 'tcp://localhost:9999'})

    # request_process.start()
    # reply_process.start()

    request_thread = threading.Thread(target=request, args=['tcp://*:9999'])
    reply_thread = threading.Thread(target=reply, args=['tcp://localhost:9999'])

    request_thread.start()
    reply_thread.start()

    request_thread.join()
    reply_thread.join()