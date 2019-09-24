from faker import Faker
import numpy as np
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

    # do this once
    def seed(self):
        conn = sqlite3.connect("data/users.db")
        conn.execute('''CREATE TABLE IF NOT EXISTS users(
                            id INTEGER PRIMARY KEY AUTOINCREMENT, 
                            name TEXT NOT NULL,
                            email TEXT NOT NULL,
                            primary_loc TEXT NOT NULL,
                            home_ip TEXT NOT NULL,
                            user_since TEXT NOT NULL 
                            )''')
        # may have to change user_since's dtype to DATETIME in Spark
        self.create_user(conn)
        conn.close()

    def run(self):
        self.seed()
        _thread.start_new_thread(self.stream_users, ())
        _thread.start_new_thread(self.stream_user_logs, ())
        time.sleep(self.interval * 5)  # FIXME: hack for rate at which users and user_logs populate
        while True:
            pass

    def create_user(self, conn):
        user = User(None, self.fake.name(), self.fake.email(), self.fake.latlng(), self.fake.ipv4(), datetime.now())
        # try:
        query = 'INSERT INTO users(name, email, primary_loc, home_ip, user_since) \
                 VALUES ("{}", "{}", "{}", "{}", "{}")'.format({user.name}, {user.email}, {user.primary_loc}, {user.home_ip}, {user.user_since})
        # transform latlng column in Spark to '76.192702, 90.369953' format
        conn.execute(query)
        conn.commit()
        logging.info(f"CREATE -- {user}")
        # except sqlite3.OperationalError:
        #     pass
        return user

    def get_random_user(self, conn):
        (count,) = conn.execute("SELECT COUNT(*) FROM users").fetchone()
        offset = random.randint(0, count - 1)
        row = conn.execute(f"SELECT * FROM users LIMIT {offset}, 1").fetchone()
        return User(*row)

    log_count = 1

    def create_user_log(self, conn):
        conn = sqlite3.connect("data/users.db")
        user = self.get_random_user(conn)
        activity = [0, 1]
        weight = [0.8, 0.2]
        # user_state = random.choice(activity, p=[0.9, 0.05])
        user_state = np.random.choice(activity, p=weight)

        log = Log(self.log_count, user.id, user.primary_loc, user.home_ip, datetime.now(), user_state).to_json()
        self.log_count += 1
        return log

    def stream_users(self):
        conn = sqlite3.connect("data/users.db")
        while True:
            for _ in range(1, 5):
                self.create_user(conn)
            time.sleep(self.interval * 5)

    def stream_user_logs(self):
        conn = sqlite3.connect("data/users.db")
        while True:
            f = open('data/user_logs.log', 'a')
            for _ in range(1, random.randint(1, 20)):
                log = self.create_user_log(conn)
                logging.info(f"APPEND -- {log}")
                f.write(log)
                f.write("\n")

            f.close
            time.sleep(self.interval * 1)

    def user_on_the_move(self, user_id, route):
        pass


class User:
    def __init__(self, _id, name, email, primary_loc, home_ip, user_since):
        self.id = _id
        self.name = name
        self.email = email
        self.primary_loc = primary_loc
        self.home_ip = home_ip
        self.user_since = user_since

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
        return f'{{ "id": {self.id}, \
                    "user_id": {self.user_id}, \
                    "curr_loc": {self.curr_loc}, \
                    "curr_ip": {self.curr_ip}, \
                    "created_at": {self.created_at} \
                    "log": {self.log} \
                    }}'


if __name__ == "__main__":
    if len(sys.argv) > 1:
        DataStreamer(int(sys.argv[1])).run()
    else:
        DataStreamer(500).run()

