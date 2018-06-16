#!/usr/bin/python3.6

"""
Cloud utilities etl
"""

import logging
import boto3
import os
import uuid
import urllib.parse

from boto3.s3.transfer import S3Transfer

from etl import common


class Queue(object):
    """
    Queuing class
    """

    def __init__(self, queue_name, logger=None):
        """
        Queuing class constructor
        """
        self.queue_name = queue_name
        self.logger = logger or logging.getLogger(__name__)
        self.resource = boto3.resource('sqs')
        self.client = boto3.client('sqs')

    def put_message(self, payload):
        """
        Queue a message to SQS
        """
        queue = self.resource.get_queue_by_name(QueueName=self.queue_name)
        response = queue.send_message(QueueUrl=queue.url,
                                      MessageBody=payload)
        self.logger.info(response)

    def get_url(self):
        """
        Get url of SQS queue
        """
        queue = self.resource.get_queue_by_name(QueueName=self.queue_name)
        return queue.url

    def purge(self):
        """
        Remove all messages in the queue
        """
        response = self.client.purge_queue(QueueUrl=self.get_url())
        self.logger.info(response)

    def get_message(self, max_number_of_messages=1,
                    visibility_timeout=43200, wait_time_seconds=1):
        """
        Get a message from SQS
        """
        key = 'Messages'
        message = None
        response = self.client.receive_message(
            QueueUrl=self.get_url(),
            MaxNumberOfMessages=max_number_of_messages,
            VisibilityTimeout=visibility_timeout,
            WaitTimeSeconds=wait_time_seconds
        )
        if key in response:
            message = response['Messages'][0]
        return message

    def remove_message(self, message):
        """
        Remove message from queue
        """
        response = self.client.delete_message(
            QueueUrl=self.get_url(),
            ReceiptHandle=message['ReceiptHandle']
        )
        return response

'''
    def queue_s3_bucket(self, bucket_name, key_name, queue_name):
        """
        Queue the item in S3 bucket to SQS
        
        Example : s3://bucket_name/key_name/items
                  Each items will be send to SQS for further processing
        
        """
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
            self.send_message(queue_name, payload)

'''


class CloudStorage(object):
    """
    Worker class
    """

    def __init__(self, logger=None):
        self.logger = logger or logging.getLogger(__name__)
        self.client = boto3.client('s3')

    def download_file(self, bucket_name, key_name, destination_path):
        """
        Download a file from S3
        """
        self.logger.info('Download file...')
        transfer = S3Transfer(self.client)
        self.logger.info('Download %s to %s...' % (urllib.parse.urljoin(bucket_name, key_name),
                                                   destination_path))
        transfer.download_file(bucket_name, key_name, destination_path)

    def upload_file(self, source_path, bucket_name, key_name):
        """
        Upload a file to S3
        """
        transfer = S3Transfer(self.client)
        self.logger.info('Upload %s to %s...' % (source_path,
                                                 urllib.parse.urljoin(bucket_name, key_name)))
        transfer.upload_file(source_path, bucket_name, key_name)

    def upload_files(self, directory_path, bucket_name, key_name, file_extension='*'):
        """
        Loop and upload files in a directory
        """
        for root, dirs, files in os.walk(directory_path):
            for local_file in files:
                file_name = common.FileName(local_file)
                if file_extension == file_name.get_file_extension() or file_extension == '*':
                    file_path = os.path.join(root, local_file)
                    file_name = os.path.basename(local_file)
                    # upload unzip files to s3
                    self.logger.info('Upload %s to %s/%s' % (file_path, bucket_name, os.path.join(key_name, file_name)))
                    self.upload_file(file_path, bucket_name, os.path.join(key_name, file_name))

    def unzip_file(self, source_bucket_name, source_key_name,
                   destination_bucket_name, destination_key_name,
                   file_extension='*', temp_directory='/tmp'):

        integration = common.Integration()

        # unique uuid to be use as key
        unique_id = str(uuid.uuid4())

        # local path to be use to unzip data
        directory_path = os.path.join(temp_directory, unique_id)

        # make dir
        integration.make_directory(directory_path)

        # download
        self.download_file(source_bucket_name, source_key_name,
                           os.path.join(directory_path, os.path.basename(source_key_name)))

        # unzip
        integration.unzip_files(directory_path, directory_path)

        # upload the unzip files to s3
        self.upload_files(directory_path, destination_bucket_name,
                          destination_key_name, file_extension)
