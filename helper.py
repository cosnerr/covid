
import requests
import time
import os
import psycopg2
import sqlalchemy
import logging
from dotenv import load_dotenv
load_dotenv(verbose=True)


# Import and set sensitive variables.
usr = os.getenv('USERNAME')
pwd = os.getenv('PASSWORD')
host = os.getenv('HOST')
port = os.getenv('PORT')
db = os.getenv('DATABASE')
schema = os.getenv('SCHEMA')
table = os.getenv('TABLE')


# Add standard logging.
logger = logging.getLogger(__name__)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(funcName)s(): %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)


def db_conn_get():
    logger.debug("Create db connection")
    try:
        conn = psycopg2.connect(
            database=db,
            user=usr,
            password=pwd,
            host=host,
            port=port
        )
        logger.debug("Database connection was successful")
        return conn
    except Exception as e:
        logger.debug(e, exc_info=True)
        return None


def engine_get():
    logger.debug("Create SqlAlchemy engine")
    try:
        engine = sqlalchemy.create_engine(f'postgresql+psycopg2://{usr}:{pwd}@{host}:{port}/{db}')
        logger.debug("SqlAlchemy engine successfully created")
        return engine
    except Exception as e:
        logger.debug(e, exc_info=True)
        return None


def call_api_append_results(api_dates):
    start_time = time.time()
    api_success = 0
    api_failure = 0
    state_by_date_dict = {}

    for api_date in api_dates:
        try:
            state_by_date = requests.request("GET", api_date, headers={}, data={})
            if state_by_date.status_code == 200:
                resp_json = state_by_date.json()
                state_by_date_dict[api_date] = resp_json
                api_success += 1
            else:
                state_by_date.raise_for_status()
        except Exception as e:
            api_failure += 1
            logger.debug(e)

    logger.debug(f'Number of successful responses: {api_success}')
    logger.debug(f'Number of error responses: {api_failure}')
    logger.debug(f'Run Time in Seconds: {time.time() - start_time}')

    return state_by_date_dict

def log_me(msg):
    logger.debug(msg)


if __name__ == '__main__':
    pass
