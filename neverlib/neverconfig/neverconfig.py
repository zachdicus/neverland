#!/usr/bin/env python
from ConfigParser import SafeConfigParser


class NeverConfig:

    def __init__(self, filename):
        parser = SafeConfigParser()
        parser.read(filename)

        self.username = parser.get("DATABASE", "username")
        self.password = parser.get("DATABASE", "password")
        self.host = parser.get("DATABASE", "host")
        self.port = parser.get("DATABASE", "port")
        self.type = parser.get("DATABASE", "type")
        self.schema = parser.get("DATABASE", "schema")
        self.nodeName = parser.get("NODE", "name")

    def __repr__(self):
        return "Username: %s \nPassword: %s\nHost: %s\nPort: %s\n" \
            "Type: %s\nSchema: %s\nNode: %s" % (self.username, self.password, self.host,
                                                self.port, self.type, self.schema, self.nodeName)
