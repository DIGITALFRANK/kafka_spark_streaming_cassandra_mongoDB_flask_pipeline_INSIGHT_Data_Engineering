from faker import Faker
from functools import reduce
from urllib.parse import parse_qs
from urllib.parse import urlparse

from datetime import datetime
import http.server
import json
import logging
import random
import re
import sqlite3
import sys
import _thread
import time
import uuid


class DataStreamer:
    def __init__(self, interval_ms=1000):
        logging.basicConfig(level=logging.INFO)
        self.interval = interval_ms * 0.001
        self.fake = Faker()

    def seed(self):
        conn = sqlite3.connect("data/.internal.db")
        conn.execute('''CREATE TABLE IF NOT EXISTS users(
                            id INTEGER PRIMARY KEY AUTOINCREMENT, 
                            name TEXT NOT NULL,
                            email TEXT NOT NULL,
                            primary_loc TEXT NOT NULL,
                            user_since DATETIME NOT NULL
                            )''')
        self.create_users(conn)
        conn.close()

    def run(self):
        self.seed()
        _thread.start_new_thread(self.stream_users, ())
        _thread.start_new_thread(self.stream_user_logs, ())
        time.sleep(self.interval * 5)  # FIXME: hack for rate at which users and user_logs populate
        while True:
            pass

    def create_users(self, conn):
        pass

    def create_user_logs(self):
        pass

    def stream_users(self):
        pass

    def stream_user_logs(self):
        pass


class User:
    def __init__(self, _id, name, email, primary_loc):
        self.id = _id
        self.name = name
        self.email = email
        self.primary_loc = primary_loc
        self.user_since = datetime.now()

    def __str__(self):
        return f"user {self.id}, {self.name}, {self.email}, {self.primary_loc}, {self.user_since}"


class Log:
    def __init__(self, _id, user_id, curr_loc, curr_ip, created_at, log):
        self.id = _id
        self.user_id = user_id
        self.curr_loc = curr_loc
        self.curr_ip = curr_ip
        self.created_at = created_at
        self.log = log

    def __str__(self):
        return f"log {self.id}, {self.user_id}, {self.curr_loc}, {self.curr_ip}, {self.created_at}, {self.log}"

    def to_json(self):
        return f'{{"id": {self.id}, {self.user_id}, {self.curr_loc}, {self.curr_ip}, {self.created_at}, {self.log}}}'


if __name__ == "__main__":
    if len(sys.argv) > 1:
        DataStreamer(int(sys.argv[1])).run()
    else:
        DataStreamer(500).run()

