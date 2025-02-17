import functools
import logging
import pickle
import random
import time

import requests

UTC_TIME_FORMAT_PARSE = "%Y-%m-%d %I:%M:%S %p %z"
UTC_TIME_FORMAT_DISPLAY = "%Y-%m-%d %I:%M:%S %p %Z"

REDIS_PUBSUB_CHANNEL_AE = "WhiskyBot Analysis Engine Notifications"


def retry(exceptions, delay=0.0, times=3):
    """
    A decorator for retrying a function call with a specified delay in case of a set of exceptions

    Parameter List
    -------------
    :param exceptions:  A tuple of all exceptions that need to be caught for retry
      e.g. retry(exception_list = (Timeout, Readtimeout))
    :param delay: Amount of delay (seconds) needed between successive retries.
    :param times: no of times the function should be retried
    """

    def outer_wrapper(func):
        @functools.wraps(func)
        def inner_wrapper(*args, **kwargs):
            final_excep = None
            for counter in range(times):
                if counter > 0:
                    time.sleep(delay * (2 ** (counter - 1)))
                try:
                    value = func(*args, **kwargs)
                    return value
                except exceptions as e:
                    logging.info("@retry decorator: " + str(e))
                    final_excep = e
                    pass

            if final_excep is not None:
                raise final_excep

        return inner_wrapper

    return outer_wrapper


# https://www.whatismybrowser.com/guides/the-latest-user-agent/

USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:87.0) Gecko/20100101 Firefox/87.0",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:88.0) Gecko/20100101 Firefox/88.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 11_3_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 11.3; rv:88.0) Gecko/20100101 Firefox/88.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:88.0) Gecko/20100101 Firefox/88.0",
    "Mozilla/5.0 (X11; Fedora; Linux x86_64; rv:88.0) Gecko/20100101 Firefox/88.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 11_3_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1 Safari/605.1.15",
]


def wobble(t):
    return random.uniform(t - (t * 0.1), t + (t * 0.1))


def build_request_redis_key(key_prefix, url, kwargs):
    return key_prefix.encode() + pickle.dumps([url, kwargs])


# TODO: kinda need to make an outter wrapper for this that catches any exception
#  and caches that, possibly w/ a different timeout.
#  - Right now we're not caching actual errors, and so we end up re-requesting every
#    time, which violates the point of this layer.
@retry(
    exceptions=(requests.Timeout, requests.HTTPError, ConnectionError, TimeoutError),
    delay=1.0,
    times=3,
)
def get_request_redis(
    redis_conn, key_prefix, timeout, url, cache=True, cookies=None, **kwargs
):
    key = build_request_redis_key(key_prefix, url, kwargs)
    cache_hit = False
    r = redis_conn.get(key)
    if not r or not cache:
        logging.info("Fetching: %s, %s, %s", key_prefix, url, kwargs)
        headers = {
            "User-agent": random.choice(USER_AGENTS),
            "Accept-Language": "en-US,en;q=0.5",
        }
        r = requests.get(
            url, params=kwargs, timeout=(2, 15), headers=headers, cookies=cookies
        )
        r.raise_for_status()
        redis_conn.set(key, pickle.dumps(r), ex=int(timeout))

        # Throttle requests
        time.sleep(wobble(1))
    else:
        logging.info("Cache hit for: %s, %s, %s", key_prefix, url, kwargs)
        r = pickle.loads(r)
        cache_hit = True
    return r, cache_hit


def extend_request_redis(redis_conn, key_prefix, timeout, url, cookies=None, **kwargs):
    key = build_request_redis_key(key_prefix, url, kwargs)
    ttl = redis_conn.ttl(key)
    if ttl < timeout / 2:
        logging.info("Extending: %s, %s, %s, %d", key_prefix, url, kwargs, timeout)
        redis_conn.expire(key, timeout)


FIVE_MIN_IN_SECONDS = 60 * 5
ONE_HOUR_IN_SECONDS = 60 * 60
ONE_DAY_IN_SECONDS = ONE_HOUR_IN_SECONDS * 24
ONE_MONTH_IN_SECONDS = ONE_DAY_IN_SECONDS * 30
ONE_YEAR_IN_SECONDS = ONE_DAY_IN_SECONDS * 365
