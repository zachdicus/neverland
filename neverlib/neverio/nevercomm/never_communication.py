#!/usr/bin/env python
import socket
import struct

class NeverCommunication:
    """Communication library for sending messages between nodes"""

    def __init__(self, host, port, should_bind=True):
        # Create a TCP/IP socket
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        # Connect the socket to the port where the server is listening
        server_address = (host, port)
        if should_bind:
            self.socket.bind(server_address)
        else:
            self.socket.connect(server_address)

    def start_listening(self):
        self.socket.listen(1)

    def wait_for_message(self):
        connection, client_address = self.socket.accept()
        message = NeverCommunication.receive_message(connection)
        connection.close()
        return message

    def close(self):
        self.socket.close()

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
        while len(data) < message_size:
            packet = connection.recv(message_size - len(data))
            if not packet:
                return None
            data += packet

        return data
