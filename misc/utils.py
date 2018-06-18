#!/usr/bin/python3.6

"""
Generic utilities etl
"""

import os
import zipfile
import glob
import logging

import psycopg2
import boto3

from boto3.s3.transfer import S3Transfer

from etl.decorator import profiler
from etl.decorator import exception

LOGGER = logging.getLogger('distribute_files')
SWITCH = 'on'


def queue_s3_bucket(bucket_name, key_name, queue_name):
    """
    Queue the item in S3 bucket to SQS
    """
    client = boto3.client('s3')

    metadata = client.list_objects(Bucket=bucket_name, Prefix=key_name)['Contents']
    for m_data in metadata:
        # construct payload
        # nsw_test_csv.csv goes to test table
        key = os.path.basename(m_data['Key'])
        destination_table = remove_file_type_from_file_name(key)
        destination_table = remove_region_code(destination_table)
        destination_table = remove_authority_code(destination_table)
        destination_table = get_file_name(destination_table)
        # specific queue to import file to database
        if queue_name == 'import_file':
            payload = generate_import_file_payload(bucket_name, m_data['Key'],
                                                   destination_table)

        # queue the payload
        queue_message(queue_name, payload)


'''
def setup_logger():
    """
    Setup LOGGER object to handle logging
    """

    logger = logging.getLogger('distribute_files')
    stream_handler = logging.StreamHandler()
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
    return logger
'''
