#!/usr/bin/env python3

import argparse
import io
import sys
import json
from datetime import datetime
import collections.abc as collections

state_count = 0

import pkg_resources
# from jsonschema.validators import Draft4Validator
import logging as logger
import boto3

s3_client = boto3.client('s3')

def emit_state(state):
    singer_state = {'type': 'STATE', 'value': state}
    if state is not None:
        line = json.dumps(singer_state)
        logger.debug('Emitting state {}'.format(line))
        sys.stdout.write("{}\n".format(line))
        sys.stdout.flush()

def flatten(d, parent_key='', sep='__'):
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, collections.MutableMapping):
            items.extend(flatten(v, new_key, sep=sep).items())
        else:
            items.append((new_key, str(v) if type(v) is list else v))
    return dict(items)

def persist_lines(config, lines):
    """
    Given a stream of lines (normally from stdin) and a configuration in json format
    consume the stream by putting each singer record to an S3 bucket
    """

    def put_records(records, stream, bucket):
        """
        Puts a list of records in an S3 bucket, and logs a singer STATE message on
        every batch upload
        """
        s3_client.put_object(
            Key='{}/{}'.format(stream, datetime.now().isoformat()),
            Bucket=bucket,
            Body='\n'.join(records)
        )
        global state_count
        state_count = state_count + len(records)
        emit_state({'count' : state_count, 'stream': stream})

    def offload_buffers(buffer_dict, max_size, bucket):
        for stream, records in buffer_dict.items():
            if len(records) >= max_size and len(records) > 0:
                put_records(records, stream, bucket)
                data_to_be_upload[stream] = []

    data_to_be_upload = {}

    buffer_size = config.get('buffer_size', 1000)
    bucket_name = config['bucket_name']

    # low level 'for' loop
    # https://www.programiz.com/python-programming/iterator
    iter_obj = iter(lines)

    # Loop over lines from stdin
    while True:
        try:
            line = next(iter_obj)
            try:
                o = json.loads(line)
            except json.decoder.JSONDecodeError:
                logger.error("Unable to parse:\n{}".format(line))
                raise

            if 'type' not in o:
                raise Exception("Line is missing required key 'type': {}".format(line))
            t = o['type']

            if t == 'RECORD':
                if 'stream' not in o:
                    raise Exception("Line is missing required key 'stream': {}".format(line))
            else:
                raise Exception('Only "RECORD" messages are supported')

            # get the stream name, as it will be used to build the s3 prefix
            stream = o['stream']
            # on a first version we simply want to be able to dump the raw message
            # data as is to the bucket
            raw_data = json.dumps(o['record'])
            # for the moment append the raw record to a list to be put in s3 in a
            # batch upload at the end
            if data_to_be_upload.get(stream) is None:
                data_to_be_upload[stream] = []
            data_to_be_upload[stream].append(raw_data)

            # if we reached the batch size upload to S3
            offload_buffers(data_to_be_upload, buffer_size, bucket_name)

            yield raw_data
        except StopIteration:
            # if StopIteration is raised, break from loop: https://www.programiz.com/python-programming/iterator
            # flush remaining lines to S3
            offload_buffers(data_to_be_upload, 0, bucket_name)
            break

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', help='Config file', default='config.json')
    args = parser.parse_args()

    if args.config:
        with open(args.config) as input:
            config = json.load(input)
    else:
        config = {}

    # make sure provided bucket exists or is accesible
    s3_client.head_bucket(Bucket=config['bucket_name'])

    input = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')

    for line in persist_lines(config, input):
        # we don't want to do anything with the operator return value
        pass

    logger.debug("Exiting normally")


if __name__ == '__main__':
    main()
