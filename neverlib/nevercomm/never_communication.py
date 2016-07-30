#!/usr/bin/env python
import socket
import struct
import time

class NeverCommunication:
    """Communication library for sending messages between nodes"""

    def __init__(self, host, port, should_bind=True):
        # Create a TCP/IP socket
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        #self.socket.setblocking(0)
        self.connection = None
        self.client_address = None
        self.socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

        # Connect the socket to the port where the server is listening
        server_address = (host, port)
        if should_bind:
            self.socket.bind(server_address)
        else:
            self.socket.connect(server_address)

    def start_listening(self):
        self.socket.listen(1)

    def wait_for_message(self):
        self.connection, self.client_address = self.socket.accept()
        message = NeverCommunication.receive_message(self.connection)
        return message

    def close_connection(self):
        self.connection.close()

    def close(self):
        self.socket.close()

    def send_message_over_connection(self, message):
        message = struct.pack('>I', len(message)) + message
        self.connection.sendall(message)

    def send_message(self, message):
        # Prefix each message with a 4-byte length (network byte order)
        message = struct.pack('>I', len(message)) + message
        self.socket.sendall(message)

    @staticmethod
    def receive_message(connection):
        # Read message length and unpack it into an integer
        message_length = connection.recv(4)
        if not message_length:
            return None
        message_length = struct.unpack('>I', message_length)[0]

        # Read the message data
        return NeverCommunication.receive_all(connection, message_length)

    @staticmethod
    def receive_all(connection, message_size):
        data = ''
        start_time = time.time()
        while len(data) < message_size:
            packet = connection.recv(message_size - len(data))
            if not packet:
                return None
            data += packet
        seconds = time.time() - start_time
        print "Receive All: %f" % seconds
        return data
