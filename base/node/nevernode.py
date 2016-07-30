#!/usr/bin/env python2.7
import json
import logging
import os
import sys
import time
from multiprocessing import Process, Manager

from sqlalchemy import create_engine, or_
from sqlalchemy.orm import sessionmaker

import neverlib.nevercomm.never_communication as ncom
from neverlib.neverclass.NeverClasses import NeverSource, NeverNode
from neverlib.neverconfig.neverconfig import NeverConfig


def query_sources(data, lock, config, logger):
    # Create the connection string
    connection_string = NeverSource.create_connection_string(config.type, config.username, config.password,
                                                             config.host, config.port, config.schema)

    while True:
        # Create the engine and session
        eng = create_engine(connection_string)
        Session = sessionmaker(bind=eng)
        session = Session()

        # Get the node information
        nodes = session.query(NeverNode).filter(NeverNode.name == config.nodeName)
        if nodes.count() > 1:
            logger.error("There is more than one node named %s", config.nodeName)
            sys.exit(1)

        # Get every source for the node
        for source in session.query(NeverSource).filter(or_(NeverSource.primaryNodeId == nodes[0].nodeId,
                                                            NeverSource.secondaryNodeId == nodes[0].nodeId)):
            # Keep track of the time it takes to query each source
            start_time = time.time()
            local_data = source.query()
            lock.acquire()
            data[source.name] = local_data
            lock.release()
            seconds = time.time() - start_time
            size = sys.getsizeof(data[source.name])

            # Save the source statistics
            source.insert_stats(session, nodes[0].nodeId, size, seconds)
            session.commit()

        print "done"
        # FIXME configuration
        time.sleep(60)

        # Close the session
        session.close()


def messages(data, lock, logger):
    # Connect to the port and start listening for requests
    comms = ncom.NeverCommunication('localhost', 10000)
    comms.start_listening()
    i = 0

    while True:
        print "What now %d" % i

        local = json.loads(comms.wait_for_message())
        print time.time()
        lock.acquire()
        data_to_return = data['Users'][2]
        comms.send_message_over_connection("")
        s = 0
        #for key in data:
        #    print "Key %s, Value %s" % (key)
        #    s += 1
        #    if s > 10:
        #        break
        lock.release()


def main():
    # Global variables
    manager = Manager()
    data = manager.dict()
    lock = manager.Lock()

    # Setup the logger
    FORMAT = '%(asctime)-15s %(message)s'
    logging.basicConfig(format=FORMAT)
    logger = logging.getLogger("NeverNode")

    try:
        config = NeverConfig(os.environ['NEVER_CONFIG'])
    except Exception:
        logger.error("Unable to load the configuration file")
        sys.exit(1)

    # Main loop
    p1 = Process(target=query_sources, args=(data,lock, config, logger))
    p2 = Process(target=messages, args=(data, lock, logger))
    p1.start()
    p2.start()
    p1.join()
    p2.join()


if __name__ == '__main__':
    main()