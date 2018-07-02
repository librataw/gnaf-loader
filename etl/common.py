import os
import logging
import glob
import json
import zipfile


class Payload(object):
    """
    Payload class
    """

    def __init__(self, logger=None):
        """
        Payload class constructor
        """
        self.logger = logger or logging.getLogger(__name__)

    def generate_import_file_in_s3_payload(self, bucket_name, key_name, destination_table):
        """
        Generate payload to import file in cloud storage to database
        """
        # construct payload
        payload = '{'
        payload += '"bucket_name" : "' + bucket_name + '",'
        payload += '"key_name" : "' + key_name + '",'
        payload += '"instruction" : "import_file ",'
        payload += '"details" : {"destination_table" : "' + destination_table + '"}'
        payload += '}'
        return payload


class FileName(object):
    """
    FileName class
    """

    def __init__(self, logger=None):
        """
        FileName class constructor
        """
        self.logger = logger or logging.getLogger(__name__)

    def remove_file_extension(self, text):
        """
        Remove file extension from file name (e.g test_csv.csv become test.csv)
        """
        # get file type
        file_extension = self.get_file_extension(text)
        f_name = self.get_file_name(text)
        result = text
        search_item = '_{0}'.format(file_extension)
        if f_name.endswith(search_item):
            # test_csv.csv will become test.csv
            result = self.trim_end(text, search_item)
        return result

    def get_file_extension(self, text):
        """
        Get file extension from file name
        """
        file_extension = os.path.splitext(text)[1]
        if file_extension.startswith('.'):
            file_extension = file_extension[1:]
        return file_extension

    def get_file_name(self, text):
        """
        Get file name without file extension
        """
        return os.path.splitext(text)[0]

    def remove_region_code(self, text):
        """
        Remove region code from file name (e.g nsw_test.csv become test.csv)
        """
        trimmed = False
        # list of region code
        region_codes = ['ACT', 'NSW', 'NT', 'OT', 'QLD', 'SA', 'TAS', 'VIC', 'WA']
        # add underscore after region code
        search_items = [region_code + '_' for region_code in region_codes]
        # get file type
        result = text

        for search_item in (si for si in search_items if trimmed is False):
            # if the file name starts with any of the search item
            if result.startswith(search_item):
                result = self.trim_start(text, search_item)
                # stop checking once trimmed
                trimmed = True
        return result

    def trim_start(self, text, search_item):
        """
        Remove search_item from beginning of file name (e.g authority_code_test.csv become test.csv)
        """
        # get file type
        f_name = self.get_file_name(text)
        result = text
        if f_name.startswith(search_item):
            result = result[len(search_item):]
        return result

    def trim_end(self, text, search_item):
        """
        Remove search_item at the end of file name (e.g code_test.csv become test.csv)
        """
        # get file type
        file_extension = self.get_file_extension(text)
        f_name = self.get_file_name(text)
        result = text
        if f_name.endswith(search_item):
            result = '{0}.{1}'.format(f_name.replace(search_item, ''), file_extension)
        return result


class Integration(object):
    """
    Integration class
    """
    def __init__(self, logger=None):
        """
        Integration class constructor
        """
        self.logger = logger or logging.getLogger(__name__)

    def make_directory(self, directory_path):
        """
        Make directory if not exist
        """
        if not os.path.exists(directory_path):
            os.makedirs(directory_path)

    # unzip files in a directory
    def unzip_files(self, directory_path, destination_path):
        """
        loop each files and unzip it
        """
        for local_file in glob.glob(os.path.join(directory_path, '*.zip')):
            self.unzip_file(local_file, destination_path)

    # unzip a single file
    def unzip_file(self, zip_file_path, destination_path):
        """
        Unzip a single file
        """
        zip_ref = zipfile.ZipFile(zip_file_path, 'r')
        zip_ref.extractall(destination_path)
        zip_ref.close()