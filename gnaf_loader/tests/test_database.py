import unittest
import shutil
import os
import uuid
import psycopg2
import testing.postgresql

from sqlalchemy import create_engine

from context import gnaf_loader


class TestDatabase(unittest.TestCase):
    """
    Testing Database class
    """

    def test_set_connection(self):
        """
        Test set connection
        """
        with testing.postgresql.Postgresql() as postgresql:
            create_engine(postgresql.url())
            dsn = postgresql.dsn()
            database = gnaf_loader.database.Database()
            connection = database.set_connection(dsn['host'], dsn['port'], dsn['database'], dsn['user'], None)
            self.assertEqual(str(type(connection)), '<class \'psycopg2.extensions.connection\'>')

    def test_importing_a_text_file(self):
        """
        Test imprting file to database
        """
        with testing.postgresql.Postgresql() as postgresql:
            create_engine(postgresql.url())
            dsn = postgresql.dsn()
            database = gnaf_loader.database.Database()
            database.connection = psycopg2.connect(**dsn)
            cursor = database.connection.cursor()
            cursor.execute('create table test_table (col1 varchar(20), col2 varchar(20));')

            # create test directory in tmp folder
            root_path = '/tmp/'
            directory_name = str(uuid.uuid4())
            directory_path = os.path.join(root_path, directory_name)
            os.mkdir(directory_path)

            # create test file in test directory
            test_file_path = os.path.join(directory_path, 'test.file')
            f = open(test_file_path, 'w')
            f.write('col1|col2\n')
            f.write('test1|test2')
            f.close()

            # test the function
            database.import_file(test_file_path, 'test_table')
            cursor.execute('select * from test_table;')
            row = cursor.fetchone()

            # clean up
            shutil.rmtree(directory_path)

            self.assertEqual(row[0], 'test1')
            self.assertEqual(row[1], 'test2')


if __name__ == '__main__':
    unittest.main()
