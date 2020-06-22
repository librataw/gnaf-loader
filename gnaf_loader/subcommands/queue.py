#!/usr/bin/python3.6
"""
This module is use to :
    - queue each item in S3 folder to SQS
"""
import logging
import os

from gnaf_loader import cloud


def setup_logger():
    """
    Setup LOGGER object to handle logging
    """

    logger = logging.getLogger('queue_bucket')
    stream_handler = logging.StreamHandler()
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
    return logger


def queue_bucket():
    """
    Main function
    """
    queue_name = 'etl'
    bucket_name = 'data.catalog'
    key_name = 'gnaf/b4e22bad-91df-4c88-9ae2-a16e245079c3'
    action_type = 'import_file'
    logger = setup_logger()

    # queue s3 bucket to sqs
    distributor = cloud.Distributor(logger)
    logger.info('Start queueing item in %s to %s...' % (os.path.join(bucket_name, key_name), queue_name))
    logger.info('Action type is %s...' % (action_type))

    distributor.queue_items(bucket_name, key_name, queue_name, action_type)


if __name__ == '__main__':
    queue_bucket()
