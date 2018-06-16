#!/usr/bin/python3.6

"""
This module is use to :
    - queue each item in S3 folder to SQS
"""
import logging

from etl.utils import queue_s3_bucket


def main():
    """
    Main function
    """

    # the queue to be used to import the files to database
    queue_name = 'import_file'

    # unique uuid to be use as key
    unique_id = '6fa7ad0b-10c0-4795-a224-d0097a343f73'

    # bucket in s3 to put the unzipped gnaf files
    archive_bucket = 'data.catalog'

    # location in s3 bucket to put the unzipped gnaf files
    archive_key = 'gnaf'

    # setup logging
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    # create a logging format
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # setup handler
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)

    # add the handlers to the logger
    logger.addHandler(handler)

    # queue s3 bucket to sqs
    queue_s3_bucket(archive_bucket, '{0}/{1}'.format(archive_key, unique_id),
                    queue_name)


# runner
if __name__ == '__main__':
    main()
