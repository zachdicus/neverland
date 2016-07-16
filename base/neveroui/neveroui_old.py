from flask import Flask, request, session, g, redirect, url_for, abort, \
    render_template, flash
import os
from sqlalchemy import create_engine
from sqlalchemy.sql import text
from sqlalchemy.orm import sessionmaker
from neverlib.neverclass.NeverClasses import NeverSource, NeverSourceSize

app = Flask(__name__)

# Read the base configuration from the environment
user_pass = str(os.environ['NEVERLAND_DB']).split('/')
engine_type = os.environ['NEVERLAND_DB_TYPE']
host_port = os.environ['NEVERLAND_HOST']
schema = os.environ['NEVERLAND_SCHEMA']

connection_string = engine_type + "://" + user_pass[0] + ":" + user_pass[1] + "@" + host_port + "/" + schema
eng = create_engine(connection_string)

Session = sessionmaker(bind=eng)
session = Session()

@app.route("/")
def hello():
    #for source in session.query(NeverSource):
    #    print "test"

    #return str(app.root_path)
    return render_template('main.html')


@app.errorhandler(404)
def page_not_found(e):
    return render_template("404.html")

if __name__ == "__main__":
    app.run()