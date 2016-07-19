#!/usr/bin/env python
from sqlalchemy import create_engine
from sqlalchemy.sql import text
from sqlalchemy.orm import sessionmaker
from neverlib.neverclass.NeverClasses import NeverSource, NeverSourceSize
import os
from sqlalchemy.engine import Engine
import time
import logging
import sys

#logging.basicConfig()
#logger = logging.getLogger("myapp.sqltime")
#logger.setLevel(logging.FATAL)

# Read the base configuration from the environment
user_pass = str(os.environ['NEVERLAND_DB']).split('/')
engine_type = os.environ['NEVERLAND_DB_TYPE']
host_port = os.environ['NEVERLAND_HOST']
schema = os.environ['NEVERLAND_SCHEMA']

connection_string = engine_type + "://" + user_pass[0] + ":" + user_pass[1] + "@" + host_port + "/" + schema
eng = create_engine(connection_string)

Session = sessionmaker(bind=eng)
session = Session()

data = dict()
counter = 0
while True:
    for source in session.query(NeverSource):

        print time.localtime()
        data2 = source.query()

        size = NeverSourceSize(source.sourceId, 1, sys.getsizeof(data2))
        session.merge(size)
        session.commit()
        print time.localtime()

        time.sleep(45)

#@event.listens_for(Engine, "before_cursor_execute")
#def before_cursor_execute(conn, cursor, statement,
#                        parameters, context, executemany):
#    conn.info.setdefault('query_start_time', []).append(time.time())
#    logger.debug("Start Query: %s", statement)

#@event.listens_for(Engine, "after_cursor_execute")
#def after_cursor_execute(conn, cursor, statement,
#                        parameters, context, executemany):
#    total = time.time() - conn.info['query_start_time'].pop(-1)
#    logger.debug("Query Complete!")
#    logger.debug("Total Time: %f", total)