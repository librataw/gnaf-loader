"""
Message utilities etl
"""

import logging
import boto3
import os

from etl import common
from etl import cloud


class Distributor(object):
    """
    Distributor class
    """

    def __init__(self, logger=None):
        """
        Distributor class constructor
        """
        self.logger = logger or logging.getLogger(__name__)

    def queue_items(self, bucket_name, key_name, queue_name, action_type):
        """
        Queue items in S3 bucket to SQS.
        Not recursive. Will queue only the items directly in the bucket.
        """
        client = boto3.client('s3')
        payload = None

        metadata = client.list_objects(Bucket=bucket_name, Prefix=key_name)['Contents']
        for m_data in metadata:
            # construct payload
            # nsw_test_csv.csv goes to test table
            key = os.path.basename(m_data['Key'])
            file_name = common.FileName()
            destination_table = file_name.remove_file_extension(key)
            destination_table = file_name.remove_region_code(destination_table)
            destination_table = file_name.trim_start(destination_table, 'authority_code')
            destination_table = file_name.get_file_name(destination_table)

            # specific queue to import file to database
            if action_type == 'import_file':
                p = common.Payload()
                payload = p.generate_import_file_in_s3_payload(bucket_name, m_data['Key'],
                                                       destination_table)

            # queue the payload
            queue = cloud.Queue(queue_name)
            queue.put_message(payload)
