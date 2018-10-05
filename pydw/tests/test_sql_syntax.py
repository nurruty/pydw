import unittest
from SQLServer import SQLServer
from DBMS import DBMS_TYPE

sql = SQLServer()


class TestSQLServerSyntax(unittest.TestCase):

    def test_create_table_1(self):
        result = sql.create_table(
            table_name = 'Person', 
            column_names = ['Name', 'Age', 'Hobby'],
            column_types = ['varchar(10)', 'numeric(3)', 'varchar(20)'],
            column_nullable = [False, False, True]
        )
        print(result)
    
    def test_create_table_2(self):
        result = sql.create_table(
            table_name = 'Person', 
            column_names = ['Name', 'Age', 'Hobby'],
            column_types = ['varchar(10)', 'numeric(3)', 'varchar(20)'],
            column_nullable = [False, False, True],
            key = ['Name']
        )
        print(result)

    def test_create_table_3(self):
        self.assertTrue(True)

    def test_drop_table(self):
        result = sql.drop_table('Person')
        self.assertEqual(result,'DROP TABLE Person;')

    def test_add_column(self):
        pass
    