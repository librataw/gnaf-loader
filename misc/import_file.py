#!/usr/bin/python3.6

"""
This module is use to :
    - Get message from queue
    - Download file
    - Import file to database
"""

import os
import json

from etl.utils import get_message
from etl.utils import remove_message
from etl.utils import download_file_from_s3
from etl.utils import import_file
from etl.utils import get_connection


def main():
    """
    Main function
    """

    # the queue to be used to import the files to database
    queue_name = 'import_file'

    # download_path
    download_path = '/tmp/'

    # database
    server_name = 'libratadatabase.cekbxwggjjt7.ap-southeast-2.rds.amazonaws.com'
    database_name = 'gnaf'
    user_name = 'librata'
    password = 'password'

    # get message
    message = get_message(queue_name)
    if message is not None:
        message_body = message['Body']
        message_body_json = json.loads(message_body)

        # download file
        file_path = os.path.join(download_path, os.path.basename(message_body_json['key_name']))
        download_file_from_s3(message_body_json['bucket_name'],
                              message_body_json['key_name'],
                              file_path)

        # import file to database
        connection = get_connection(server_name, database_name, user_name, password)
        import_file(connection, file_path, message_body_json['destination_table'])

        # remove message from queue
        remove_message(queue_name, message)


# runner
if __name__ == '__main__':
    main()
