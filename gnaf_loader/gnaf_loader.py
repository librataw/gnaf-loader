#!/usr/bin/env python

import logging
import click
import os
import uuid
import json

from etl import cloud, database


def setup_logger():
    """
    Setup LOGGER object to handle logging
    """

    logger = logging.getLogger('archive')
    stream_handler = logging.StreamHandler()
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
    return logger


@click.group()
def cli():
    pass


@cli.command()
@click.argument('source_bucket')
@click.argument('source_file_name')
@click.argument('destination_bucket')
@click.argument('destination_folder')
def decompress(source_bucket, source_file_path, destination_bucket, destination_folder):
    """
    Decompress file from S3 staging bucekt to another S3 bucket
    """
    # setup logger
    logger = setup_logger()
    # the file extension of gnaf data
    file_extension = 'psv'
    # location in s3 bucket to put the unzipped gnaf files
    destination_key = os.path.join(destination_folder, str(uuid.uuid4()))

    cs = cloud.CloudStorage(logger)

    logger.info('Start unzipping file from cloud storage...')
    cs.unzip_file(source_bucket, source_file_path, destination_bucket, destination_key, file_extension)
    logger.info('Finish archiving files...')
    logger.info('Files are archived in S3 %s' % os.path.join(destination_bucket, destination_key))


@cli.command()
@click.argument('queue_name')
@click.argument('bucket_name')
@click.argument('key_name')
def queue(queue_name, bucket_name, key_name):
    """
    Queue items in s3 bucket to SQS
    """
    logger = setup_logger()

    action_type = 'import_file'

    # queue s3 bucket to sqs
    distributor = cloud.Distributor(logger)
    logger.info('Start queueing item in %s to %s...' % (os.path.join(bucket_name, key_name), queue_name))
    logger.info('Action type is %s...' % action_type)

    distributor.queue_items(bucket_name, key_name, queue_name, action_type)


@cli.command()
@click.argument('queue_name')
@click.argument('temp_dir')
@click.argument('db_host')
@click.argument('db_name')
@click.argument('db_username')
@click.argument('db_password')
@click.argument('db_port')
def import_data(queue_name, temp_dir, db_host, db_name, db_username, db_password, db_port):
    """
    Import file queued in SQS to PostgreSQL database
    """

    logger = setup_logger()

    cloudstorage = cloud.CloudStorage(logger)
    queue = cloud.Queue(queue_name, logger)
    db = database.Database(logger)

    logger.info('Setting up database connection to %s...' % (db_host))
    db.set_connection(db_host, db_port, db_name, db_username, db_password)

    logger.info('Disable foreign key constraints checks...')
    db.disable_foreign_key_constraints()

    # get message
    logger.info('Getting message from queue %s...' % (queue_name))
    message = queue.get_message()

    while message is not None:
        logger.info('Message retrieved : %s' % (message))

        message_body = message['Body']
        message_body_json = json.loads(message_body)

        # download file
        instruction = message_body_json['instruction']
        logger.info('Message instruction : %s' % (instruction))

        if instruction == 'import_file':
            file_path = os.path.join(temp_dir, os.path.basename(message_body_json['key_name']))
            logger.info('Downloading file from %s to %s...' % (os.path.join(message_body_json['bucket_name'],
                                                                            message_body_json['key_name']),
                                                               file_path))
            cloudstorage.download_file(message_body_json['bucket_name'],
                                       message_body_json['key_name'],
                                       file_path)

            # import file to database
            logger.info('Importing file %s to %s...' % (file_path, message_body_json['details']['destination_table']))
            db.import_file(file_path, message_body_json['details']['destination_table'], truncate=True)

            # remove message from queue
            logger.info('Removing message %s...' %(message))
            queue.remove_message(message)

        # get message
        logger.info('Getting message from queue %s...' % (queue_name))
        message = queue.get_message()

    logger.info('Enable foreign key constraints checks...')
    db.enable_foreign_key_constraints()


if __name__ == '__main__':
    cli()
