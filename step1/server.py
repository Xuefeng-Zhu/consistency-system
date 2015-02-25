import sys
import json
import socket
import pickle
from functools import wraps
from threading import Thread
from Queue import Queue
from message import Message
from datetime import datetime
from random import random
from time import sleep


def load_config():
    with open('config.json') as f:
        config = json.load(f)
    return config


def thread_func(func):
    @wraps(func)
    def start_thread(*args, **kwargs):
        thread = Thread(target=func, args=args, kwargs=kwargs)
        thread.daemon = True
        thread.start()
        return thread
    return start_thread


class Server:

    def __init__(self, name):
        self.name = name
        self.config = load_config()
        self.host = socket.gethostname()
        self.port = self.config[name]['port']
        self.max_delay = self.config[name]['delay']

        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.bind((self.host, self.port))
        self.queue = Queue()

    @thread_func
    def send_message(self):
        while True:
            try:
                content, destination = raw_input("Send: ").split()
            except ValueError:
                print "Your input is invalid"
                continue
            send_time = datetime.now()
            message = Message(self.name, destination, content, send_time)
            raw_message = pickle.dumps(message)
            dest_port = self.config[destination]['port']
            self.socket.sendto(raw_message, (self.host, dest_port))
            print 'Send "%s" to %s, system time is %s' \
                % (content, destination, str(send_time))

    @thread_func
    def receive_message(self):
        while True:
            raw_message, addr = self.socket.recvfrom(2048)
            message = pickle.loads(raw_message)
            self.queue.put(message)

    @thread_func
    def delay_message(self):
        message = self.queue.get()
        delay_time = self.max_delay * random()
        sleep_time = delay_time - \
            (datetime.now() - message.send_time).total_seconds()
        if sleep_time > 0:
            sleep(sleep_time)
        print 'Received "%s" from %s, Max delay is %d s,' \
            'system time is %s' % (message.content, message.sender,
                                   self.max_delay, str(datetime.now()))


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print "Usage: python server.py server_name"
        exit(0)

    try:
        server = Server(sys.argv[1])
    except KeyError:
        print "Please provide valid server name"
    send_thread = server.send_message()
    receive_thread = server.receive_message()
    delay_thread = server.delay_message()

    send_thread.join()
    receive_thread.join()
    delay_thread.join()
