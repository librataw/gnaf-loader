#!/usr/bin/python3.6

"""
This module is use to :
    - Download zip file from S3 staging bucket
    - Unzip file
    - Upload the unzipped file to archive bucket
"""

import uuid
import os

from etl import cloud


def main():
    """
    Main function
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
    archive_key = 'gnaf'

    cloudstorage = cloud.CloudStorage()

    cloudstorage.unzip_file(staging_bucket_name, file_name, archive_bucket, archive_key, file_extension)


# runner
if __name__ == '__main__':
    main()