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

    def import_file(self, file_path, table_name, delimiter='|', header=True):
        """
        Import text file to database
        """
        sql = 'COPY {0} FROM STDIN WITH(FORMAT CSV, DELIMITER \'{1}\', \
              HEADER {2})'.format(table_name, delimiter, header)
        local_file = open(file_path)
        cursor = self.connection.cursor()
        cursor.copy_expert(sql, local_file)
        self.connection.commit()
        cursor.close()
