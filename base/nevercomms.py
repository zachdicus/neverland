#!/usr/bin/env python
import neverlib.neverio.nevercomm.never_communication as ncom
import json
import neverlib.neverclass.NeverClasses as nc


# Connect to the port and start listening for requests
comms = ncom.NeverCommunication('localhost', 10000)
comms.start_listening()

while True:
    data = json.loads(comms.wait_for_message())
    print 'received "%s"' % data
    source = nc.NeverSource(data)
    print source.source_name