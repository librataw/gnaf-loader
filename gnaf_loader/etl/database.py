import psycopg2
import logging


class Database(object):
    """
    Database related class
    """
    def __init__(self, logger=None):
        """
        Database class constructor
        """
        self.logger = logger or logging.getLogger(__name__)
        self.connection = None

    def set_connection(self, server_name, port, database_name, user_name, password):
        """
        Get database connection
        """
        self.connection = psycopg2.connect(host=server_name, port=port, dbname=database_name,
                                user=user_name, password=password)
        return self.connection

    def disable_foreign_key_constraints(self):
        with self.connection.cursor() as cursor:
            cursor.execute('SET session_replication_role = \'replica\'')

    def enable_foreign_key_constraints(self):
        with self.connection.cursor() as cursor:
            cursor.execute('SET session_replication_role = \'origin\'')

    def import_file(self, file_path, table_name, delimiter='|', header=True, truncate=False):
        """
        Import text file to database
        """
        with self.connection.cursor() as cursor:

            if truncate:
                cursor.execute('TRUNCATE TABLE {0} CASCADE'.format(table_name))

            if header:
                with open(file_path, 'r') as f:
                    column_names = f.readline()[:-1].replace(delimiter, ',')
                sql = 'COPY {0} ({1}) FROM STDIN WITH(FORMAT CSV, DELIMITER \'{2}\', \
                      HEADER {3})'.format(table_name, column_names, delimiter, header)

            else:
                sql = 'COPY {0} FROM STDIN WITH(FORMAT CSV, DELIMITER \'{1}\', \
                      HEADER {2})'.format(table_name, delimiter, header)

            with open(file_path, 'r') as local_file:
                cursor.copy_expert(sql, local_file)
                self.connection.commit()

    def truncate_table(self, table_name):
        """
        Truncate table
        """
        with self.connection.cursor() as cursor:
            cursor.execute('TRUNCATE TABLE {0} CASCADE'.format(table_name))
        self.connection.commit()

    def close_connection(self):
        if self.connection:
            self.connection.close()