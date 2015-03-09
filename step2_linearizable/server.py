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

    def __init__(self, name, callback):
        self.name = name
        self.callback = callback #callback is a function of form (sender, message)
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
    def send_message(self, content, destination):
        send_time = datetime.now()
        message = Message(self.name, destination, content, send_time)
        raw_message = pickle.dumps(message)

        try:
            dest_port = self.config[destination]['port']
        except KeyError:
            print "Error: invalid destination; perhaps there's a config problem?"
            return

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
            self.callback(sender, message)



class LinearizableWrapper:

    def __init__(self, name):
        self.name = name
        if self.name == "Server":
            try:
                self.server = Server(name, self.server_callback)
                #todo: server init
            except KeyError:
                print "Error: server not defined in config file"
                sys.exit(-1)
        else:
            try:
                self.server = Server(name, self.client_callback)
                # todo: client init
            except KeyError:
                print "Error: client " + name + " not defined in config file"
                sys.exit(-1)
        self.store = {}

    def server_callback(self, sender, message):
        """
        Iterates through all clients and bcasts the message
        Implements linearizability by using algorithm for
        totally ordered broadcast laid out in lecture

        totally ordered bcast is achieved by a system
        of "blocking" until original process ack is received
        """
        payload = message.content
        payload_tokens = payload.split()
        if payload_tokens[0] == "insert" or payload_tokens[0] == "update":
            pass
        elif payload_tokens[0] == "get":
            pass
        elif payload_tokens[0] == "ack":
            pass
        elif payload_tokens[0] == "val":
            pass
        elif payload_tokens[0] == "delete":
            pass

    def client_callback(self, sender, message):
        payload = message.content
        payload_tokens = payload.split()
        if self.name == sender:
            #in this case, if it's a read we need to send our value
            #else if it's a write, we need to send an ack
            if payload_tokens[0] == "insert" or payload_tokens[0] == "update":
                #here, we'll send an ack since the write has returned to us
                self.send_ack()
            elif payload_tokens[0] == "get":
                self.send_val(self.store[int(payload_tokens[1])])
        if payload[0] == "insert":
            #insert new value into the key-value store
            self.store.insert(int(payload_tokens[1], int(payload_tokens[2])))
        elif payload[0] == "update":
            try:
                self.store.update(int(payload_tokens[1], int(payload_tokens[2])))
            except KeyError:
                print("FATAL ERROR: received read request for nonexistent key")
                sys.exit(-1)
        elif payload[0] == "delete":
            try:
                self.store.delete(int(payload_tokens[1]))
            except KeyError:
                print("FATAL ERROR: received delete request for nonexistent key")
                sys.exit(-1)

    def send_ack(self):
        self.server.send_message("ack " + self.name, "Server")

    def send_val(self, val):
        self.server.send_message("val " + str(val) + " " + self.name, "Server")

if __name__ == '__main__':
    pass #stubbed out, add some logic to start node
