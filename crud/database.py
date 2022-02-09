"""
This script connects to MY SQL database and performs CRUD Operations
"""
import ast
import csv
import os

from dotenv import load_dotenv
from mysql.connector import connect, Error

load_dotenv(os.path.join(os.path.dirname(__file__), 'data/environment.env'))


HOST = "localhost"

class Connection:
    """
    implements Create read update and delete functions
    """
    my_db = None
    table_name = ''
    file_name = ''
    table_columns = []
    data_type_list = []
    database = "File_Storage"
    
    @staticmethod
    def connect():
        """
        makes a mysql database connection and store it inside my_db class variable
        :return: None
        """
        try:
            Connection.my_db = connect(
                    host=HOST,
                    user=os.environ.get('secretUser'),
                    password=os.environ.get('secretKey')
                    # user='local',
                    # password='local'
            )
        except Error as db_error:
            print('error'+str(db_error))

    @staticmethod
    def upload_file(name):
        """
        Creates a new table in DB with given filename and insert the file content
        to that table

        :param name: Filename that has to be uploaded
        :return: None

        Note: filename will be treated as TABLE_NAME
        """
        # os.path.join(os.path.dirname(__file__),
        with open(os.path.join(os.path.dirname(__file__), name), 'r',
                  encoding="utf-8") as file:
            table_name = "_".join(name.split('.'))
            Connection.table_name = table_name
            reader = csv.reader(file)
            statement = Connection.create_statement(reader)
            cursor = Connection.my_db.cursor()
            cursor.execute(f"SHOW databases LIKE '{Connection.database}'")
            if cursor.fetchone() is None:
                cursor.execute(f"create Database {Connection.database}")
            cursor.execute(f'DROP TABLE IF EXISTS {Connection.database}.{table_name };')
            cursor.execute(statement)
        with open(os.path.join(os.path.dirname(__file__), name), 'r',
                  encoding="utf-8") as file:
            reader = csv.reader(file)
            Connection.insert_statement(reader)
            Connection.my_db.commit()

    @staticmethod
    def load_columns():
        """
        Saves the column_names of the table in table_columns list
        :return: None
        """
        cursor = Connection.my_db.cursor()
        cursor.execute('SHOW COLUMNS FROM ' + Connection.database + '.' + Connection.table_name)
        data = cursor.fetchall()
        for column in data:
            Connection.table_columns.append(column[0])

    @staticmethod
    def datatype(value, current_type):
        """
        determines the type of the given value
        :param value: any number or string
        :param current_type: current_data_type of the value
        :return: sql row_data type
        """
        try:
            # Evaluates numbers to an appropriate type, and strings an error
            type_evaluated = ast.literal_eval(value)
        except ValueError:
            return 'varchar'
        except SyntaxError:
            return 'varchar'
        if any([isinstance(type_evaluated, float), isinstance(type_evaluated, float)]):
            if (isinstance(type_evaluated, int)) and current_type not in ['float', 'varchar']:
                # Use smallest possible int type
                if (-32768 < type_evaluated < 32767) and current_type not in ['int', 'bigint']:
                    return 'smallint'
                if (-2147483648 < type_evaluated < 2147483647) and current_type not in ['bigint']:
                    return 'int'
                return 'bigint'
            if isinstance(type_evaluated, float) and current_type not in ['varchar']:
                return 'decimal'
        else:
            return 'varchar'

    @staticmethod
    def create_statement(reader):
        """
        reads the file column names and returns the create table query
        :param reader: csv file reader object
        :return: create table query
        """
        longest, columns, type_list = [], [], []
        Connection.table_columns = columns = Connection.load_metadata(columns, longest,
                                                                      reader, type_list)
        create_query = f'create table {Connection.database}.{Connection.table_name}('
        for i in range(len(columns)):
            create_query = (create_query + f'\n{columns[i].lower()} {type_list[i]}({longest[i]}),')
        create_query = create_query[:-1] + f',\nPRIMARY KEY ({columns[0]}, {columns[1]}));'
        return create_query

    @staticmethod
    def load_metadata(columns, longest, reader, type_list):
        """
        extracts column names from file content ,determines the row_data type of each column
        and max length required for each column field

        :param columns: a list to store column names
        :param longest: a list to store length of each column field
        :param reader: a csv reader object to read the file contents
        :param type_list: a list to store datatype of each column
        :return: list of columns
        """
        for row in reader:
            if len(columns) == 0:
                columns = row
                for col in row:
                    longest.append(0)
                    type_list.append('')
                    if col.count('.') > 0:
                        columns.insert(columns.index(col), "_".join(col.split('.')))
                        columns.pop(columns.index(col))
            else:
                for i in range(len(row)):
                    # NA is the csv null value
                    if type_list[i] == 'varchar' or row[i] == 'NA':
                        pass
                    else:
                        var_type = Connection.datatype(row[i], type_list[i])
                        type_list[i] = var_type
                    if len(row[i]) > longest[i]:
                        longest[i] = len(row[i])
        Connection.data_type_list = type_list
        return columns

    @staticmethod
    def insert_statement(reader):
        """
        reads the file content and returns the corresponding insert statement
        :param reader: csv file reader
        :return: None
        """
        i = 0
        result = ''
        cursor = Connection.my_db.cursor()
        for row in reader:
            if i == 0:
                pass
            else:
                insert_query = 'INSERT INTO ' + Connection.database + '.' + Connection.table_name + ' values ('
                j = 0
                for cell in row:
                    if Connection.data_type_list[j] == 'varchar':
                        insert_query += f"'{cell}',"
                    else:
                        insert_query += f"{cell},"
                    j += 1
                insert_query = insert_query[:-1] + ');'
                result = result + insert_query
        #         cursor.execute(insert_query)
            i += 1
        # Connection.my_db.commit()
        return result

    @staticmethod
    def select_statement(record_id=None):
        """
        fetches the rows from the table
        :return: list of rows
        """
        cursor = Connection.my_db.cursor()
        data = []
        if record_id is None:
            cursor.execute(f'SELECT * FROM {Connection.database}.{Connection.table_name}')
        else:
            cursor.execute(f"SELECT * FROM {Connection.database}.{Connection.table_name} WHERE id = '{record_id}'")
        data.append(Connection.table_columns)
        data.append(cursor.fetchall())
        return data

    @staticmethod
    def update(data, record_id):
        """
        takes row row_data , id and updates the row in the database
        :param data: row row_data to be updated
        :param record_id: id of the record
        :return: None
        """
        cursor = Connection.my_db.cursor()
        update_stat = f'UPDATE {Connection.database}.{Connection.table_name} SET '
        statement = ''
        for key, value in data.items():
            statement += str(key) + ' = \'' + str(value) + '\' ,'
        update_stat = update_stat + statement[:-1] + ' WHERE id = \'' + str(record_id) + '\''
        cursor.execute(update_stat)
        Connection.my_db.commit()

    @staticmethod
    def add_row(row_data):
        """
        adds a new row to table
        :param row_data: row data to be updated
        :return: None
        """
        cursor = Connection.my_db.cursor()
        insert_query = f'INSERT INTO {Connection.database}.{Connection.table_name} values('
        i = 0
        for value in row_data.values():
            if Connection.data_type_list[i] == 'varchar':
                insert_query = (insert_query + f"'{value}',")
            else:
                insert_query = (insert_query + f"{value},")
        insert_query = insert_query[:-1] + ');'
        return insert_query
        # cursor.execute(insert_query)
        # Connection.my_db.commit()

    @staticmethod
    def delete_row(record_id):
        """
        :param record_id: unique id of the record to be deleted
        :return: table data and msg
        """
        cursor = Connection.my_db.cursor()
        cursor.execute(f"SELECT EXISTS(SELECT * from {Connection.database}.{Connection.table_name} "
                       f"WHERE id = '{record_id}')")
        result = cursor.fetchone()
        if result[0] != 1:
            msg = f"Record with id = {record_id} does not exist"
        else:
            cursor.execute(f"DELETE from {Connection.database}.{Connection.table_name} WHERE id='{record_id}'")
            msg = 'Record Deleted Successfully'
        data = Connection.select_statement()
        Connection.my_db.commit()
        return data, msg


class Credentials:

    def __init__(self, username, password):
        self.__username = username
        self.__password = password

    def set(self):
        with open(os.path.join(os.path.dirname(__file__), f'data{os.sep}environment.env'), "w") as file:
            file.write('secretUser='+self.__username+'\nsecretKey='+self.__password)
        load_dotenv(os.path.join(os.path.dirname(__file__), f'data{os.sep}environment.env'))


if __name__ == '__main__':
    obj = Connection()
    obj.connect()
    curso = obj.my_db.cursor()
    curso.execute("SHOW databases LIKE 'file_storage'")
    print(curso.fetchone())
