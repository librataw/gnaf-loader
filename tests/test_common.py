import unittest
import uuid
import os
import zipfile
import shutil

from context import etl


class TestFileName(unittest.TestCase):
    """
    Testing FileName class
    """

    def test_get_file_extension(self):
        """
        Test getting the file extension from file name
        e.g: thefile.txt should return txt
        """
        filename = etl.common.FileName('thefile.txt')
        result = filename.get_file_extension()
        self.assertEqual(result, 'txt')

    def test_remove_file_extension_in_file_name(self):
        """
        Test removing file extension in file name
        e.g: thefile_txt.txt will be thefile.txt
        """
        filename = etl.common.FileName('thefile_txt.txt')
        result = filename.remove_file_extension()
        self.assertEqual(result, 'thefile.txt')

    def test_get_file_name(self):
        """
        Test getting the file name from full file name
        e.g: thefile.txt should return thefile
        """
        filename = etl.common.FileName('thefile.txt')
        result = filename.get_file_name()
        self.assertEqual(result, 'thefile')

    def test_remove_region_code_in_file_name(self):
        """
        Test removing region code from the beginning of file name
        e.g: NSW_thefile.txt should return thefile.txt
        """
        filename = etl.common.FileName('NSW_thefile.txt')
        result = filename.remove_region_code()
        self.assertEqual(result, 'thefile.txt')

    def test_remove_double_region_code_in_file_name(self):
        """
        Test removing double region code at the beginning of file name
        e.g: NSW_OT_thefile.txt should return OT_thefile.txt
        """
        filename = etl.common.FileName('NSW_OT_thefile.txt')
        result = filename.remove_region_code()
        self.assertEqual(result, 'OT_thefile.txt')

    def test_remove_unknown_region_code_in_file_name(self):
        """
        Test removing unknown region code in file name
        e.g: unknownregion_thefile.txt should return as is
        """
        filename = etl.common.FileName('unknownregion_thefile.txt')
        result = filename.remove_region_code()
        self.assertEqual(result, 'unknownregion_thefile.txt')

    def test_trim_start(self):
        """
        Test remove any text from the beginning of the file name
        e.g: start_thefile.txt become thefile.txt
        """
        filename = etl.common.FileName('start_thefile.txt')
        result = filename.trim_start('start_')
        self.assertEqual(result, 'thefile.txt')

    def test_trim_end(self):
        """
        Test remove any text from the end of file name
        e.g: thefile_end.txt become thefile.txt
        """
        filename = etl.common.FileName('thefile_end.txt')
        result = filename.trim_end('_end')
        self.assertEqual(result, 'thefile.txt')


class TestPayload(unittest.TestCase):
    def setUp(self):
        self.payload = etl.common.Payload()

    def test_generate_import_file_in_s3_payload(self):
        """
        Test import file payload creation
        """
        expected_result = '{"bucket_name" : "test_bucket","key_name" : "test_key","instruction" : "import_file ","details" : {"destination_table" : "test_table"}}'
        result = self.payload.generate_import_file_in_s3_payload('test_bucket', 'test_key', 'test_table')
        self.assertEqual(result, expected_result)


class TestIntegration(unittest.TestCase):
    def setUp(self):
        self.integration = etl.common.Integration()

    def test_make_directory(self):
        """
        Test making directory
        """
        directory_path = os.path.join('/tmp/', str(uuid.uuid4()))
        self.integration.make_directory(directory_path)
        self.assertTrue(os.path.exists(directory_path))

    def test_unzip_files(self):
        """
        Test unzipping zip files inside a folder.
        """
        # create test directory in tmp folder
        root_name = str(uuid.uuid4())
        root_path = os.path.join('/tmp/', root_name)
        os.mkdir(root_path)

        # create first zip file
        directory_name = str(uuid.uuid4())
        directory_path = os.path.join(root_path, directory_name)
        os.mkdir(directory_path)

        test_file_path = os.path.join(directory_path, 'test.file')
        open(test_file_path, 'a').close()

        zip_file_path = directory_path + '.zip'
        zip = zipfile.ZipFile(zip_file_path, 'w', zipfile.ZIP_DEFLATED)

        for root, dirs, files in os.walk(directory_path):
            for file in files:
                zip.write(os.path.join(root, file))

        zip.close()

        # create second zip file
        directory_name = str(uuid.uuid4())
        directory_path = os.path.join(root_path, directory_name)
        os.mkdir(directory_path)

        test_file_path = os.path.join(directory_path, 'test.file')
        open(test_file_path, 'a').close()

        zip_file_path = directory_path + '.zip'
        zip = zipfile.ZipFile(zip_file_path, 'w', zipfile.ZIP_DEFLATED)

        for root, dirs, files in os.walk(directory_path):
            for file in files:
                zip.write(os.path.join(root, file))

        zip.close()

        # unzip
        unzip_file_path = os.path.join(root_path, 'test')
        self.integration.unzip_files(root_path, unzip_file_path)
        exists = os.path.exists(unzip_file_path)

        # clean up
        shutil.rmtree(root_path)

        # test
        self.assertTrue(exists)

    def test_unzip_file(self):
        """
        Test unzipping a single file
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

        # unzip
        unzip_file_path = os.path.join(root_path, directory_name + '_test')
        self.integration.unzip_file(zip_file_path, unzip_file_path)
        exists = os.path.exists(unzip_file_path)

        # clean up
        shutil.rmtree(unzip_file_path)
        shutil.rmtree(directory_path)

        # test
        self.assertTrue(exists)


if __name__ == '__main__':
    unittest.main()