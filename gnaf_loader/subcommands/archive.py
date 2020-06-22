#!/usr/bin/python3.6
"""
This module is use to :
    - Download zip file from S3 staging bucket
    - Unzip file
    - Upload the unzipped file to archive bucket
"""

import logging
import os
import uuid

from gnaf_loader import cloud


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


def archive():
    """
    Archive function
    """
    # the bucket name in s3 where the zipped gnaf file lives
    staging_bucket_name = 'staging.space'

    # name of the zipped gnaf file
    file_name = 'feb18gnafpipeseparatedvalue20180219141901.zip'

    # the file extension of gnaf data
    file_extension = 'psv'

    # bucket in s3 to put the unzipped gnaf files
    archive_bucket = 'data.catalog'

    # location in s3 bucket to put the unzipped gnaf files
    archive_key = os.path.join('gnaf', str(uuid.uuid4()))

    logger = setup_logger()

    cloudstorage = cloud.CloudStorage(logger)

    logger.info('Start unzipping file from cloud storage...')
    cloudstorage.unzip_file(staging_bucket_name, file_name, archive_bucket, archive_key, file_extension)
    logger.info('Finish archiving files...')
    logger.info('Files are archived in S3 %s' % os.path.join(archive_bucket, archive_key))


if __name__ == '__main__':
    archive()
