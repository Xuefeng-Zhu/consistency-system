import sys
import json
import socket
import pickle
from functools import wraps
from threading import Thread
from Queue import Queue
from message import Message
from datetime import datetime, timedelta
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

        self.queues = {}
        for s in self.config:
            self.queues[s] = Queue()

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

            try:
                dest_port = self.config[destination]['port']
            except KeyError:
                print "Please provide valid server name"

            self.socket.sendto(raw_message, (self.host, dest_port))
            print 'Send "%s" to %s, system time is %s' \
                % (content, destination, str(send_time))

    @thread_func
    def receive_message(self):
        while True:
            raw_message, addr = self.socket.recvfrom(2048)
            message = pickle.loads(raw_message)
            delay_time = self.max_delay * random()
            message.deliver_time = message.send_time + \
                timedelta(seconds=delay_time)
            self.queues[message.sender].put(message)

    @thread_func
    def delay_message(self, sender):
        queue = self.queues[sender]
        while True:
            message = queue.get()
            sleep_time = (message.deliver_time -
                          datetime.now()).total_seconds()
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

    delay_threads = {}
    for s in server.config:
        delay_threads[s] = server.delay_message(s)

    send_thread.join()
    receive_thread.join()
    for s in server.config:
        delay_threads[s].join()
