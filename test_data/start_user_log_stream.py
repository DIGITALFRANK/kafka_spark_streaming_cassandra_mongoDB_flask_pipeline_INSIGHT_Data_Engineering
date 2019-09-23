from faker import Faker
from faker.providers import Profile
import time
import random
import os
import numpy as np
from datetime import datetime, timedelta


# example Faker generated log
LINE = """ \
{remote_addr} - - [{time_local}] "{request_type} {request_path} HTTP/1.1" [{status}] \
{body_bytes_sent} "{http_referer}" "{http_user_agent}" \
"""


def generate_log_line():
    fake = Faker()
    now = datetime.now()
    remote_addr = fake.ipv4()
    time_local = now.strftime('%d/%b/%Y:%H:%M:%S')
    request_type = random.choice(["GET", "POST", "PUT"])
    request_path = "/" + fake.uri_path()

    status = np.random.choice([200, 401, 404], p=[0.9, 0.05, 0.05])
    body_bytes_sent = random.choice(range(5, 1000, 1))
    http_referer = fake.uri()
    http_user_agent = fake.user_agent()

    log_line = LINE.format(
        remote_addr=remote_addr,
        time_local=time_local,
        request_type=request_type,
        request_path=request_path,
        status=status,
        body_bytes_sent=body_bytes_sent,
        http_referer=http_referer,
        http_user_agent=http_user_agent
    )

    return log_line


# log_id has to auto-increment and pick up from its position when stream is stopped/restarted

# log_user_id needs to be a random choice from the `id` column of the users SQLite table
# this has to be dynamic as new users are continuously added to the table

# My log
def new_log_line():
    fake = Faker()
    fake.add_provider(Profile)
    activity = [0, 1]

    log_line = {
        "log": {
            "log_id": "12035",  # unique log id, may be a time unique id (timeuuid)
            "log_user_id": "1205670",  # log user id, select random from SQLite users table
            "log_ip": fake.ipv4(),
            "log_datetime": datetime.now(),
            # "log_location": fake.latlng(),
            "log_curr_loc": fake.current_location(),
            "log": random.choice(activity)
        }
    }

    return log_line






