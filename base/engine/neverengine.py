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

logging.basicConfig()
logger = logging.getLogger("NeverEngine")
logger.setLevel(logging.DEBUG)

# Read the base configuration from the environment
user_pass = os.environ['NEVERLAND_DB'].split('/')
engine_type = os.environ['NEVERLAND_DB_TYPE']
host_port = os.environ['NEVERLAND_HOST'].split(':')
schema = os.environ['NEVERLAND_SCHEMA']

connection_string = NeverSource.build_connection_string(engine_type, user_pass[0], user_pass[1],
                                                        host_port[0], host_port[1], schema)

while True:
    eng = create_engine(connection_string)
    Session = sessionmaker(bind=eng)
    session = Session()

    session.close()
    time.sleep(60)
