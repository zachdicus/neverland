#!/usr/bin/env python
import socket
import sys
import struct


def recv_msg(sock):
    # Read message length and unpack it into an integer
    raw_msglen = recvall(sock, 4)
    if not raw_msglen:
        return None
    msglen = struct.unpack('>I', raw_msglen)[0]
    # Read the message data
    return recvall(sock, msglen)


def recvall(sock, n):
    # Helper function to recv n bytes or return None if EOF is hit
    data = ''
    while len(data) < n:
        packet = sock.recv(n - len(data))
        if not packet:
            return None
        data += packet

    return data

# Create a TCP/IP socket
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

# Bind the socket to the port
server_address = ('localhost', 10000)
print >>sys.stderr, 'starting up on %s port %s' % server_address
sock.bind(server_address)

# Listen for incoming connections
sock.listen(1)

while True:
    # Wait for a connection
    print >>sys.stderr, 'waiting for a connection'
    connection, client_address = sock.accept()

    try:
        print >> sys.stderr, 'connection from', client_address
        data = recv_msg(connection)
        print 'received "%s"' % data
        # Receive the data in small chunks and retransmit it
        #while True:
        #    data = connection.recv(16)
        #    print >> sys.stderr, 'received "%s"' % data
        #    if data:
        #        print >> sys.stderr, 'sending data back to the client'
        #        connection.sendall(data)
        #    else:
        #        print >> sys.stderr, 'no more data from', client_address
        #        break

    finally:
        # Clean up the connection
        connection.close()
