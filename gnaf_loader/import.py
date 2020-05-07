#!/usr/bin/python3.6

"""
This module is use to :
    - Get message from queue
    - Download file
    - Import file to database
"""

import os
import json
import logging

from gnaf_loader import cloud
from gnaf_loader import database


def setup_logger():
    """
    Setup LOGGER object to handle logging
    """

    logger = logging.getLogger('import')
    stream_handler = logging.StreamHandler()
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
    return logger


def import_data():
    """
    Main function
    """

    queue_name = 'etl'
    download_path = '/tmp/'

    server_name = 'localhost'
    database_name = 'places'
    user_name = 'etl_user'
    password = 'password'
    port = '5432'

    logger = setup_logger()

    cloudstorage = cloud.CloudStorage(logger)
    queue = cloud.Queue(queue_name, logger)
    db = database.Database(logger)

    logger.info('Setting up database connection to %s...' % (server_name))
    connection = db.set_connection(server_name, port, database_name, user_name, password)

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
            file_path = os.path.join(download_path, os.path.basename(message_body_json['key_name']))
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


# runner
if __name__ == '__main__':
    import_data()
