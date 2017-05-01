"""
This module is responsible for all database operations.
"""
import logging
import sys
import warnings
import threading

import pymysql
import pymysql.cursors

### based on Sebastian's database handler (uses pymysql)

class DatabaseHandler:
    """
    This class handles all database operations.
    """

    # pylint: disable=too-many-instance-attributes

    def __init__(self, host, user, password, db_name):
        self.host = host
        self.user = user
        self.password = password
        self.db_name = db_name
        self.logger = logging.getLogger()
        self.cnx = None
        self.data_base = None
        self.connect()
        self.condition = threading.Condition()

    def connect(self):
        """
        Connect to the database.
        """
        try:
            self.data_base = pymysql.connect(
                self.host, self.user, self.password, self.db_name,
                cursorclass=pymysql.cursors.DictCursor, charset='utf8')

            warnings.filterwarnings("error", category=pymysql.Warning)
            self.cnx = self.data_base.cursor()
            self.data_base.autocommit(True)
            self.logger.debug('Connected to database %s on %s with user %s'
                              % (self.db_name, self.host, self.user))
        except pymysql.Error as my_sql_error:
            self.logger.error(
                "Error while establishing connection to the database server [%d]: %s"
                % (my_sql_error.args[0], my_sql_error.args[1]))
            sys.exit(1)

    def close(self):
        """
        Close the connection.
        """
        self.cnx.close()
        self.data_base.close()
        self.logger.debug('Closed DB connection')


    @staticmethod
    def __build_insert_sql(self, table, objs):
        """
        This method creates an insert SQL string and returns it.
        :param table: The table in which the data shall be inserted.
        :param objs:
        :return: Insert SQL string.
        """
        if len(objs) == 0:
            return None
        key_set = set()
        key_set = [key_set.update(row.keys()) for row in objs]
        columns = [col for col in key_set]
        tuples = []
        for item in objs:
            if item:
                values = []
                for key in columns:
                    try:
                        values.append('"%key_set"' % str(item[key]).replace('"', "")
                                      if not item[key] == '' else 'NULL')
                    except KeyError:
                        values.append('NULL')
                if not all(value == 'NULL' for value in values):
                    tuples.append('(%key_set)' % ', '.join(values))
        return 'INSERT INTO `' + table + '` (' + ', '.join(
            ['`%key_set`' % column for column in columns]) \
            + ') VALUES\n' + ',\n'.join(tuples)


    @staticmethod
    def __build_select_sql(table_name):
        """
        Builds a select * statement.
        :param table_name: The table for which a select * shall be performed.
        :return: Returns the select * string.
        """
        statement = 'SELECT * FROM`' + table_name + ';'
        return statement

    #Execute SQL-statement
    def execute(self, statement):
        """
        This method will execute a plain SQL statement.
        :param statement: The statement that shall be executed.
        :return:
        """
        if statement:
            with self.condition:
                try:
                    self.logger.debug('Executing SQL-query:\n\t%s'
                                      % statement.replace('\n', '\n\t'))
                    self.cnx.execute(statement)
                    return self.cnx.fetchall()
                except pymysql.Warning as mysql_error:
                    self.logger.warning("Warning while executing statement: %s" % mysql_error)
                except pymysql.Error as mysql_error:
                    self.logger.error("Error while executing statement [%d]: %s"
                                      % (mysql_error.args[0], mysql_error.args[1]))

    def persist_dict(self, table, array_of_dicts):
        """
        Build an insert SQL statement and execute it.
        :param table: The table in which the data shall be inserted.
        :param array_of_dicts: The data that is to be inserted.
        """
        sql = self.__build_insert_sql(table, array_of_dicts)
        self.execute(sql)

    def select(self, table):
        """
        Execute a select statement
        :param table:
        :return:
        """
        sql = self.__build_select_sql(table)
        result_set = self.execute(sql)
        return result_set
