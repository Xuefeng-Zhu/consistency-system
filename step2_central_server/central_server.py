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
        self.waiting_for_ack = False
        self.acks = 0
        self.send_queue = Queue()

    @thread_func
    def send_message(self):
        while True:
            message = self.send_queue.get()
            raw_message = pickle.dumps(message)
            try:
                dest_port = self.config[message.receiver]['port']
            except KeyError:
                print "Please provide valid server name"
                continue

            self.socket.sendto(raw_message, (self.host, dest_port))
            print 'Send "%s" to %s, system time is %s' \
                % (message.content, message.receiver, str(message.send_time))

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
            if self.waiting_for_ack and not message.content == "ack":
                queue.put(message) #if we're waiting for an ack don't take any more requests until the ack comes in
                continue
            sleep_time = (message.deliver_time -
                          datetime.now()).total_seconds()
            if sleep_time > 0:
                sleep(sleep_time)
            self.handle_message(message)

    def handle_message(self, message):
        if message.content == "ack":
            print('received ack from %s' % (message.sender))
            self.acks -= 1
            print("new ack value: " + str(self.acks))
            if self.acks == 0:
                self.waiting_for_ack = False
                print("no longer waiting for acks")

        else:
            self.waiting_for_ack = True
            for s in self.config:
                if s == self.name:
                    continue
                self.acks += 1
            print("reset acks to " + str(self.acks))

            for s in self.config:
                if s == self.name:
                    continue # don't send back to self
                bmessage = Message(self.name, s, message.content, message.send_time)
                self.send_queue.put(bmessage)


if __name__ == '__main__':
    server = Server("central")

    send_thread = server.send_message()
    receive_thread = server.receive_message()

    delay_threads = {}
    for s in server.config:
        delay_threads[s] = server.delay_message(s)

    send_thread.join()
    receive_thread.join()
    for s in server.config:
        delay_threads[s].join()
