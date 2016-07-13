#!/usr/bin/env python

from flask import Flask, jsonify
from sqlalchemy import create_engine
import os
from sqlalchemy.sql import text
app = Flask(__name__)

#user_pass = str(os.environ['NEVERLAND_DB']).split('/')
#engine_type = os.environ['NEVERLAND_DB_TYPE']
#schema = os.environ['NEVERLAND_SCHEMA']


@app.route('/neverland/api/v1.0/sources', methods=['GET'])
def get_tasks():
    eng = create_engine("mysql://root:callwavenot@192.168.1.120:3306/operations")
    result = eng.execute("""SELECT
        s.Name, s.Description, IsActive, Priority, AlchemyType, Host, Port, Username, s.Schema, Password
        from Source s
        inner join operations.Database on Database.DbId = s.DbId
        inner join operations.SourceType on SourceType.SourceTypeId = s.SourceTypeId""")
    return jsonify({'sources': result})

@app.route('/neverland/api/v1.0/sources/<int:source_id>', methods=['GET'])
def get_task(source_id):
    eng = create_engine("mysql://root:callwavenot@192.168.1.120:3306/operations")
    result = eng.execute("""SELECT
        s.SourceId, s.Name, s.Description, IsActive, Priority, AlchemyType, Host, Port, Username, s.Schema, Password
        from Source s
        inner join operations.Database on Database.DbId = s.DbId
        inner join operations.SourceType on SourceType.SourceTypeId = s.SourceTypeId""")
    for source in result:
        if source['SourceId'] == source_id:
            return jsonify({'task': source})
    return jsonify({"Error": "Can not find source with id of " + source_id})


if __name__ == '__main__':
    app.run(debug=True)