import asyncio
import fnmatch
import json
import os
import time

from concurrent import futures
from datetime import timedelta
from functools import wraps


# Wrap this function around another function that you would normally
# 	call over and over again in a loop
def multithread_execute(fn, items, workers=10):
    with futures.ThreadPoolExecutor(max_workers=workers) as executor:
        results = list(executor.map(fn, items))
    return results


# timing() is a decorator function enabled by prepending @timing to other
#   function defintions within the same solution.  In addition to returning
#   the passed function's result, it also returns duration time
def timing(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        start_time = time.monotonic()
        result = f(*args, **kwargs)
        end_time = time.monotonic()
        duration = timedelta(seconds=int(end_time - start_time))
        return duration, result

    return wrapper


# parse_parameters() accepts a list of returned ssm parameters and their
#   values.  It returns a single nested dictionary that reflects the
#   hierachy of the input parameters.
def parse_parameters(records):
    result = dict()

    for keys, value in records:
        cur_dict = result
        keys_list = keys.strip("/").split("/")
        for key in keys_list:
            cur_dict = cur_dict.setdefault(key, value if key == keys_list[-1] else {})
    return result


# serialize_lists_and_dicts() accepts a list of dictionaries and serializes
#   any lists or dictionaries into a string. This is useful for writing
#   dataframes to SQL Server.
def serialize_lists_and_dicts(list_of_dict):
    for record in list_of_dict:
        for k in record:
            if isinstance(record[k], list):
                try:
                    record[k] = ",".join(record[k])
                except TypeError:
                    record[k] = json.dumps(record[k])
            if isinstance(record[k], dict):
                record[k] = json.dumps(record[k])
    return list_of_dict


# show_percent_progress() is an output-to-console formatting function
#   that takes a message, numerator, denominator, and a done flag.
#   it gives the user a percentage progress on long running steps
def show_percent_progress(message, numerator, denominator, logger):
    division = (numerator / denominator) if denominator != 0 else 1
    logger.info(
        "\r * - {}: {} ({} of {})".format(
            message, "{:.1%}".format(division), numerator, denominator
        )
    )


# create_batches() is a utility function that takes a list of items and
#   a given batch size and returns a list of lists, each with a len of
#   the batch_size or smaller
def create_batches(input, batch_size):
    return [input[i : i + batch_size] for i in range(0, len(input), batch_size)]


# pad() is a simple function returning the provided text plus additional
#   space characters until total string len equals the length parameter
def pad(text, length):
    if length > len(text):
        return text + " " * (length - len(text))
    else:
        return text


# find_files() returns a list of files on the local file system matching the
# 	input pattern for the given path
def find_files(pattern, path, recursion=True):
    result = []
    for root, dirs, files in os.walk(path):
        for name in files:
            if fnmatch.fnmatch(name, pattern):
                result.append(os.path.join(root, name))
            if not recursion:
                break
    return result


# get_prefect_user_email() instantiates a cloud client object in order to
#   return the current user's email address
def get_prefect_user_email():
    return asyncio.run(_get_prefect_user_email())


async def _get_prefect_user_email():
    from prefect.client.cloud import get_cloud_client

    client = get_cloud_client()
    r = await client.get("/me")
    return r["email"]
