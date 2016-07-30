#!/usr/bin/env python
import neverlib.nevercomm.never_communication as ncom
import time
comms = ncom.NeverCommunication('localhost', 10000, False)

try:
    message = '{"source" : { "id": "XXXX", "username":"XYZ", "group":{"id": "XXXX"}}}'
    start_time = time.time()
    print time.time()
    comms.send_message(message)
    msg = comms.receive_message(comms.socket)
    seconds = time.time() - start_time
    print "What %s" % msg
    print seconds
finally:
    comms.close()

