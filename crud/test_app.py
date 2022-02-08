"""
This scripts has unit tests for CRUD application
@Author - Mohammad Toseef
"""
import os.path
import sys
import unittest
from io import BytesIO
from flask import request
from mysql.connector.errors import IntegrityError, DatabaseError

from .database import Connection
from .app import app
from .app import connection as con_obj
UPDATE_ID = 'algorand'


class FlaskTest(unittest.TestCase):
    """
        14 unit tests written including positive and negative test cases
    """
    @classmethod
    def setUpClass(cls) -> None:
        cls.tester = app.test_client(cls)
        cls.delete_id = 'bitcoin'
        cls.update_id = 'aave'

    def setUp(self):
        self.record_to_add = {'id': 'bit', 'symbol': 'any', 'name': 'Bit', 'image': 'img',
                              'current_price': '1', 'market_cap': '12', 'market_cap_rank': '101',
                              'fully_diluted_valuation': '', 'total_volume': '333000', 'high_24h':
                              '3', 'low_24h': '1', 'price_change_24h': '7',
                              'price_change_percentage_24h': '8', 'market_cap_change_24h': '0',
                              'market_cap_change_percentage_24h': '54', 'circulating_supply': '99',
                              'total_supply': '8888', 'max_supply': '99999', 'ath': '89',
                              'ath_change_percentage': '-1', 'ath_date': '2021-11-10T14:24:11.84Z',
                              'atl': '2.5', 'atl_change_percentage': '41', 'atl_date':
                              '2015-10-20T00:00:00.000Z', 'roi': '', 'last_updated':
                              '2021-12-07T10:44:27.802Z', 'price_change_percentage_1h_in_currency':
                              '-1', 'price_change_percentage_24h_in_currency': '8', 'roi_times': '',
                              'roi_currency': '', 'roi_percentage': ''}
        self.record_to_update = {'id': 'aave', 'symbol': 'aave', 'name': 'avv',
                                 'image': 'https://assets.coingecko.com/coins/images/12645/large/'
                                          'AAVE.png?1601374110',
                                 'current_price': '192', 'market_cap': '2579411136',
                                 'market_cap_rank': '6', 'fully_diluted_valuation': '3075737373.0',
                                 'total_volume': '272328066', 'high_24h': '193', 'low_24h': '170',
                                 'price_change_24h': '20', 'price_change_percentage_24h': '11',
                                 'market_cap_change_24h': '262446667',
                                 'market_cap_change_percentage_24h': '11',
                                 'circulating_supply': '13418109', 'total_supply': '16000000.0',
                                 'max_supply': '16000000.0',
                                 'ath': '662', 'ath_change_percentage': '-71',
                                 'ath_date': '2021-05-18T21:19:59.514Z',
                                 'atl': '26', 'atl_change_percentage': '639',
                                 'atl_date': '2020-11-05T09:20:11.928Z',
                                 'roi': '', 'last_updated': '2021-12-07T10:44:03.149Z',
                                 'price_change_percentage_1h_in_currency': '0',
                                 'price_change_percentage_24h_in_currency': '11',
                                 'roi_times': '', 'roi_currency': '', 'roi_percentage': ''}
        self.filename = 'Crypto2.csv'
        self.table_name = "_".join(self.filename.split('.'))
        self.connection = Connection()
        self.connection.connect()

    def test_01_home_page_renders_successfully(self):
        """
        Makes a get request to server and checks if response is ok and
        ensures that home page is returned as html template
        :return: None
        """
        self.tester = app.test_client(self)
        response = self.tester.get('/')
        self.assertEqual(response.status_code, 200)
        self.assertIn(b'Click on the "Choose File"', response.data)

    def test_02_file_upload_successful(self):
        """
        posts the file row_data to server and checks the view if File uploaded Successfully
        :return: None
        """
        with open(os.path.join(os.path.dirname(__file__), 'data/Crypto2.csv'),
                  'rb') as file:
            data = {
                'field': 'value',
                'filename': (BytesIO(file.read()), self.filename)
            }
            # cred_obj = Credentials('root', 'pass')
            # cred_obj.set()
            response = self.tester.post('/upload_file', buffered=True, data=data,
                                        content_type='multipart/form-data',
                                        follow_redirects=True)
            self.assertIn(b'File uploaded Successfully', response.data)

    def test_03_add_view_rendered_successfully(self):
        """
        requests server to render html page to add row_data
        :return: None
        """
        response = self.tester.get('/add')
        self.assertIn(b'<h1> Please fill out the fields </h1>', response.data)

    def test_04_record_added_successfully(self):
        """
        posts one record on server to add it to the table and checks the view if
        record has been added
        :return: None
        """
        response = self.tester.post('/add', data=self.record_to_add, follow_redirects=True)
        self.assertIn(b'Record added', response.data)

    def test_05_edit_view_rendered_successfully(self):
        """
        make get request with /update/id url and check if edit page is rendered
        :return: None
        """
        response = self.tester.get(f'/update/{self.update_id}')
        self.assertIn(f'Editing {self.update_id} Record'.encode(), response.data)

    def test_06_record_updated_successfully(self):
        """
        posts the record to be updated to server and check for 'record update' message
        in the view
        :return:
        """
        response = self.tester.post(f'/update/{self.update_id}',
                                    data=self.record_to_update, follow_redirects=True)
        self.assertIn(b'Record Updated', response.data)

    def test_07_record_deleted_successfully(self):
        """
        request server to delete record for the given id and checks for 'record deleted'
        message in the view
        :return:
        """
        response = self.tester.get(f'/delete/{self.delete_id}')
        self.assertIn(b'Record Deleted', response.data)

    def test_08_add_invalid_row_in_db(self):
        """
        posts data with empty values to add to the database
        """
        for key in self.record_to_add:
            self.record_to_add[key] = ''
        # posting data with empty values to add
        with self.assertRaises(DatabaseError):
            self.tester.post('/add', data=self.record_to_add, follow_redirects=True)

    def test_09_check_updated_record_in_db(self):
        """
        make a call to database to check if record is updated in the table
        """
        cursor = con_obj.my_db.cursor()
        cursor.execute(f"SELECT * FROM {Connection.database}.{self.table_name} WHERE id= '{self.record_to_update['id']}'")
        record = cursor.fetchone()
        record_list = []
        record_list.insert(0, list(map(str, record)))
        self.assertListEqual(record_list[0], list(self.record_to_update.values()))

    def test_10_record_deleted_in_db(self):
        """
        make a call to database to check if record is deleted in the table
        """
        delete_id = 'bitcoin'
        cursor = con_obj.my_db.cursor()
        cursor.execute(f"SELECT EXISTS(SELECT * from {Connection.database}.{self.table_name} WHERE id = '"
                       f"{delete_id}')")
        result = cursor.fetchone()
        self.assertEqual(result[0], 0)

    def test_11_add_duplicate_record_in_db(self):
        """
        posts a duplicate row to server and check if the correct message is received in response
        :return None
        """
        # self.record_to_add['symbol'] = 'amp'
        with self.assertRaises(IntegrityError):
            self.tester.post('/add', data=self.record_to_add, follow_redirects=True)

    def test_12_update_duplicate_data_in_db(self):
        """
        posts invalid row_data
        :return:
        """
        # id = binancecoin , symbol = bnb already exist in database
        self.record_to_update['id'] = 'binancecoin'
        self.record_to_update['symbol'] = 'bnb'
        with self.assertRaises(IntegrityError):
            self.tester.post(f'/update/{self.update_id}',
                             data=self.record_to_update, follow_redirects=True)

    def test_13_delete_invalid_data_in_db(self):
        """
        make request to delete record with wrong id
        :return:
        """
        wrong_id = 'def'                       # this id doesn't exist in Database
        response = self.tester.get(f'/delete/{wrong_id}')
        self.assertIn(f"Record with id = {wrong_id} does not exist".encode(), response.data)

    def test_14_wrong_url(self):
        """
        makes request with wrong url
        :return: None
        """
        response = self.tester.get('jfa')
        self.assertIn(b'Not Found', response.data)


if __name__ == '__main__':
    unittest.main()
    # obj = FlaskTest()
    # obj.test_home_page_renders_successfully()
    # obj.test_file_upload_successful()
    # obj.test_add_view_rendered_successfully()
    # obj.test_record_added_successfully()
    # obj.test_edit_view_rendered_successfully()
    # obj.test_record_updated_successfully()
    # obj.test_record_deleted_successfully()
    # obj.test_check_new_record_in_db()
    # obj.test_check_updated_record_in_db()
    # obj.test_record_deleted_in_db()
    # obj.test_add_duplicate_record_in_db()
    # obj.test_update_invalid_data_in_db()
