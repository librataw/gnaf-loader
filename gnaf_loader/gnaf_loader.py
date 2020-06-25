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
@click.argument('staging_bucket')
@click.argument('file_name')
@click.argument('archive_bucket')
def archive(staging_bucket, file_name, archive_bucket):
    """
    Archive file from S3 staging bucekt to S3 archive bucket
    :param staging_bucket: S3 staging bucket
    :param file_name: file name to be archived
    :param archive_bucket: S3 archive bucket
    :return:
    """
    # setup logger
    logger = setup_logger()
    # the file extension of gnaf data
    file_extension = 'psv'
    # location in s3 bucket to put the unzipped gnaf files
    archive_key = os.path.join('gnaf', str(uuid.uuid4()))

    cs = cloud.CloudStorage(logger)

    logger.info('Start unzipping file from cloud storage...')
    cs.unzip_file(staging_bucket, file_name, archive_bucket, archive_key, file_extension)
    logger.info('Finish archiving files...')
    logger.info('Files are archived in S3 %s' % os.path.join(archive_bucket, archive_key))


@cli.command()
@click.argument('queue_name')
@click.argument('bucket_name')
@click.argument('key_name')
def queue(queue_name, bucket_name, key_name):
    """
    Queue items in s3 bucket to SQS
    :param queue_name: SQS queue name
    :param bucket_name: S3 bucket name
    :param key_name: S3 bucket key name
    :return:
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
    :param queue_name: SQS queue name
    :param temp_dir: temp directory path to hold temporary files
    :param db_host: database host name
    :param db_name: database name
    :param db_username: database user name
    :param db_password: database password
    :param db_port: database port number
    :return:
    """

    logger = setup_logger()

    cloudstorage = cloud.CloudStorage(logger)
    queue = cloud.Queue(queue_name, logger)
    db = database.Database(logger)

    logger.info('Setting up database connection to %s...' % (db_host))
    connection = db.set_connection(db_host, db_port, db_name, db_username, db_password)

    # get message
    logger.info('Getting message from queue %s...' % (queue_name))
    message = queue.get_message()

    if message is not None:
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
            db.import_file(file_path, message_body_json['details']['destination_table'])

            # remove message from queue
            logger.info('Removing message %s...' %(message))
            queue.remove_message(message)


if __name__ == '__main__':
    cli()
