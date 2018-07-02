import unittest
import uuid
import os
import boto3
import shutil
import json

from moto import mock_s3
from moto import mock_sqs
from boto3.s3.transfer import S3Transfer

from context import etl


class TestMessage(unittest.TestCase):
    """
    Testing message class
    """

    def setUp(self):
        self.s3_resource = boto3.resource('s3')
        self.s3_client = boto3.client('s3')
        self.sqs_resource = boto3.resource('sqs')
        self.sqs_client = boto3.client('sqs')

    @mock_sqs
    @mock_s3
    def test_queue_items(self):
        """
        Test queue items
        """
        # create 2 csv files
        root_path = '/tmp/'
        directory_name = str(uuid.uuid4())
        directory_path = os.path.join(root_path, directory_name)
        os.mkdir(directory_path)

        # create test file in test directory
        test_file_path_1 = os.path.join(directory_path, 'VIC_ADDRESS_ALIAS_psv.psv')
        open(test_file_path_1, 'a').close()
        test_file_path_2 = os.path.join(directory_path, 'VIC_ADDRESS_DETAIL_psv.psv')
        open(test_file_path_2, 'a').close()

        # create bucket
        bucket_name = 'test_bucket'
        self.s3_client.create_bucket(Bucket=bucket_name)

        # upload 2 csv files to s3
        key_name = 'test_folder'
        transfer = S3Transfer(self.s3_client)
        transfer.upload_file(test_file_path_1, bucket_name, os.path.join(key_name, os.path.basename(test_file_path_1)))
        transfer.upload_file(test_file_path_2, bucket_name, os.path.join(key_name, os.path.basename(test_file_path_2)))

        # create sample queue
        queue_name = 'import_file'
        q = self.sqs_resource.create_queue(QueueName=queue_name)

        # run the function
        distributor = etl.message.Distributor()
        distributor.queue_items(bucket_name, key_name, queue_name)

        # clean up
        shutil.rmtree(directory_path)

        # ensure the queue have both files
        response = self.sqs_client.receive_message(
            QueueUrl=q.url,
            MaxNumberOfMessages=1,
            VisibilityTimeout=43200,
            WaitTimeSeconds=1
        )
        message = json.loads(response['Messages'][0]['Body'])
        self.assertEqual('ADDRESS_ALIAS', message['details']['destination_table'])

        response = self.sqs_client.receive_message(
            QueueUrl=q.url,
            MaxNumberOfMessages=1,
            VisibilityTimeout=43200,
            WaitTimeSeconds=1
        )
        message = json.loads(response['Messages'][0]['Body'])
        self.assertEqual('ADDRESS_DETAIL', message['details']['destination_table'])


if __name__ == '__main__':
    unittest.main()
