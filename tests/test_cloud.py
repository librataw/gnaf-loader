import unittest
import boto3
import uuid
import os
import zipfile
import shutil
import json

from moto import mock_sqs
from moto import mock_s3
from boto3.s3.transfer import S3Transfer

from context import etl


class TestQueue(unittest.TestCase):
    """
    Testing Queue class
    """

    def setUp(self):
        """
        Setup test fixture
        """
        self.resource = boto3.resource('sqs')
        self.client = boto3.client('sqs')
        self.queue_name = 'test'
        self.queue = etl.cloud.Queue(queue_name=self.queue_name)

    @mock_sqs
    def test_get_queue_url(self):
        """
        Test getting SQS queue URL
        """
        # create queue
        # get url using moto
        test_url = self.resource.create_queue(QueueName='test').url
        # get url using our function
        url = self.queue.get_url()
        # compare
        self.assertEqual(test_url, url)

    @mock_sqs
    def test_put_message_to_queue(self):
        """
        Test putting message to SQS queue
        """
        payload = '{"key":"value"}'

        # create queue
        q = self.resource.create_queue(QueueName=self.queue_name)

        # put message using our function
        self.queue.put_message(payload)

        # get result
        response = self.client.receive_message(
            QueueUrl=q.url,
            MaxNumberOfMessages=1,
            VisibilityTimeout=43200,
            WaitTimeSeconds=1
        )

        result = response['Messages'][0]

        # compare the body only
        self.assertIsNotNone(result['Body'])
        self.assertEqual(result['Body'], payload)

    @mock_sqs
    def test_purging_queue(self):
        """
        Test purging SQS queue
        """
        payload = '{"key":"value"}'

        # create queue
        q = self.resource.create_queue(QueueName=self.queue_name)

        # send message
        q.send_message(QueueUrl=q.url,
                       MessageBody=payload)

        # purge it
        self.queue.purge()

        # get result
        response = self.client.receive_message(
            QueueUrl=q.url,
            MaxNumberOfMessages=1,
            VisibilityTimeout=43200,
            WaitTimeSeconds=1
        )

        # make sure the message is not there
        self.assertTrue('Messages' not in response)

    @mock_sqs
    def test_get_message_from_queue(self):
        """
        Test getting message from SQS queue
        """
        payload = '{"key":"value"}'

        # create queue
        q = self.resource.create_queue(QueueName=self.queue_name)

        # send message
        q.send_message(QueueUrl=q.url,
                       MessageBody=payload)

        # get result
        result = self.queue.get_message()

        # test
        self.assertIsNotNone(result)
        self.assertEqual(result['Body'], payload)

    @mock_sqs
    def test_delete_message_from_queue(self):
        """
        Test getting message from SQS queue
        """
        payload = '{"key":"value"}'

        # create queue
        q = self.resource.create_queue(QueueName=self.queue_name)

        # send message
        q.send_message(QueueUrl=q.url,
                       MessageBody=payload)

        # get result
        message = self.client.receive_message(
            QueueUrl=q.url,
            MaxNumberOfMessages=1,
            VisibilityTimeout=43200,
            WaitTimeSeconds=1
        )['Messages'][0]

        # remove message
        self.queue.remove_message(message)

        # get result
        response = self.client.receive_message(
            QueueUrl=q.url,
            MaxNumberOfMessages=1,
            VisibilityTimeout=43200,
            WaitTimeSeconds=1
        )

        # make sure the message is not there
        self.assertTrue('Messages' not in response)


class TestCloudStorage(unittest.TestCase):
    """
    Testing Cloud Storage class
    """

    def setUp(self):
        """
        Setup test fixture
        """
        self.resource = boto3.resource('s3')
        self.client = boto3.client('s3')
        self.bucket_name = 'testbucket'

    @mock_s3
    def test_upload_file_to_s3(self):
        """
        Test upload a file to s3
        """
        key_name = 'test'
        root_path = '/tmp/'
        file_name = str(uuid.uuid4())
        file_path = os.path.join(root_path, file_name)
        cloudstorage = etl.cloud.CloudStorage()

        # create test file
        open(file_path, 'a').close()

        # create bucket
        self.client.create_bucket(Bucket=self.bucket_name)

        # upload file
        cloudstorage.upload_file(file_path, self.bucket_name, key_name)

        # cleanup
        os.remove(file_path)

        # test file existance
        bucket = self.resource.Bucket(self.bucket_name)
        objects = list(bucket.objects.filter(Prefix=key_name))

        self.assertGreater(len(objects), 0)

    @mock_s3
    def test_upload_files_to_s3(self):
        """
        Test upload a file to s3
        """

        cloudstorage = etl.cloud.CloudStorage()

        key_name = 'test'
        root_name = str(uuid.uuid4())
        root_path = os.path.join('/tmp/', root_name)
        os.mkdir(root_path)

        # create test files
        file_name_1 = str(uuid.uuid4())
        file_path_1 = os.path.join(root_path, file_name_1)
        open(file_path_1, 'a').close()

        file_name_2 = str(uuid.uuid4())
        file_path_2 = os.path.join(root_path, file_name_2)
        open(file_path_2, 'a').close()

        # create bucket
        self.client.create_bucket(Bucket=self.bucket_name)

        # upload file
        cloudstorage.upload_files(root_path, self.bucket_name, key_name)

        # cleanup
        shutil.rmtree(root_path)

        # test file existance
        bucket = self.resource.Bucket(self.bucket_name)
        s3_path = os.path.join(key_name, file_name_1)
        objects = list(bucket.objects.filter(Prefix=s3_path))
        self.assertGreater(len(objects), 0)
        s3_path = os.path.join(key_name, file_name_2)
        objects = list(bucket.objects.filter(Prefix=s3_path))
        self.assertGreater(len(objects), 0)


    @mock_s3
    def test_download_file_from_s3(self):
        """
        NOT WORKING - something wrong with MOTO

        Test upload a file to s3
        """
        '''
        key_name = 'test'
        root_path = '/tmp/'
        file_name = str(uuid.uuid4())
        file_path = os.path.join(root_path, file_name)
        cloudstorage = etl.cloud.CloudStorage()
        transfer = S3Transfer(self.client)

        # create test file
        open(file_path, 'a').close()

        # create bucket
        self.client.create_bucket(Bucket=self.bucket_name)

        # upload file
        transfer.upload_file(file_path, self.bucket_name, key_name)

        # cleanup
        os.remove(file_path)

        # download file
        file_name = str(uuid.uuid4())
        file_path = os.path.join(root_path, file_name)
        transfer.download_file(self.bucket_name, key_name, file_path)

        import pdb;pdb.set_trace()
        # test file existence
        exists = os.path.exists(file_path)

        # cleanup
        #os.remove(file_path)

        self.assertTrue(exists)
        '''
        pass

    @mock_s3
    def test_unzip_file(self):
        """
        Test unzip file in s3 using local resources
        """
        # create test directory in tmp folder
        root_path = '/tmp/'
        directory_name = str(uuid.uuid4())
        directory_path = os.path.join(root_path, directory_name)
        os.mkdir(directory_path)

        # create test file in test directory
        test_file_path = os.path.join(directory_path, 'test.file')
        open(test_file_path, 'a').close()

        # create zip file
        zip_file_path = directory_path + '.zip'
        zip = zipfile.ZipFile(zip_file_path, 'w', zipfile.ZIP_DEFLATED)

        for root, dirs, files in os.walk(directory_path):
            for file in files:
                zip.write(os.path.join(root, file))

        zip.close()

        key_name = directory_name + '.zip'

        # create bucket
        self.client.create_bucket(Bucket=self.bucket_name)

        # upload file
        transfer = S3Transfer(self.client)
        transfer.upload_file(zip_file_path, self.bucket_name, key_name)

        # cleanup
        os.remove(test_file_path)
        os.remove(zip_file_path)

        # test unzip file
        cloudstorage = etl.cloud.CloudStorage()

        cloudstorage.unzip_file(self.bucket_name, key_name,
                                self.bucket_name, directory_name, file_extension='file')

        # test file existence
        bucket = self.resource.Bucket(self.bucket_name)

        objects = list(bucket.objects.filter(Prefix=os.path.join(directory_name, 'test.file')))
        self.assertGreater(len(objects), 0)


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
        distributor = etl.cloud.Distributor()
        distributor.queue_items(bucket_name, key_name, queue_name, 'import_file')

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
