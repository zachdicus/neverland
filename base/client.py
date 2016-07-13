#!/usr/bin/env python
import neverlib.neverio.nevercomm.never_communication as ncom

comms = ncom.NeverCommunication('localhost', 10000, False)

try:
    message = '{"source" : { "id": "XXXX", "username":"XYZ", "group":{"id": "XXXX"}}}'
    comms.send_message(message)
finally:
    comms.close()

