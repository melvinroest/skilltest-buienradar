import requests
import json
import time
import logging
import sys

URL = "https://data.buienradar.nl/2.0/feed/json"
FILENAME = "buienradar.ndjson"
MAX_EXCEPTIONS = 5
TIME_BETWEEN_REQUESTS = 60 * 5
REQUEST_TIMEOUT = 30
EXCEPTION_TIMEOUT = 30

logging.basicConfig(level=logging.INFO, format='collectBuienradarData: %(asctime)s - %(levelname)s - %(message)s')
exception_amount = 0

logging.info("started")
while True:
    logging.info("started request")

    try:
        r = requests.get(URL, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        data = r.json()

        with open(FILENAME, 'a') as f:
            f.write(json.dumps(data) + '\n')
            logging.info(f"saved request to {FILENAME}")
    except Exception as e:
        exception_amount += 1
        logging.exception("Couldn't handle fetching request or serializing it or writing it to a file")
        if exception_amount >= MAX_EXCEPTIONS:
            logging.error("exiting due to 5 consecutive exceptions")
            sys.exit(1)
        logging.info("Sleeping for 30 seconds")
        time.sleep(EXCEPTION_TIMEOUT)
        continue

    exception_amount = 0
    logging.info("finished request")
    time.sleep(TIME_BETWEEN_REQUESTS)

