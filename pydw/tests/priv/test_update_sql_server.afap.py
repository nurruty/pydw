import unittest
import pymssql
from Sql_server import Sql_server
from DMS import DMS_TYPE

dw_con = pymssql.connect(server='rafap62', user='iadev', password='Pdtyynqsn2018_', database='Dimensiones_DW')
dw = dw_con.cursor()

sql = Sql_server(DMS_TYPE.SQL_SERVER)

class TestSQLServer(unittest.TestCase):

  def test_update_1(self):
      dw.execute(sql.update(
        "LKP_TEST",
        ["TEST_DESC"],
        ["'test'"]
      ))
      dw_con.commit()
      dw.execute(sql.select(
        ["TEST_DESC"],
        ["LKP_TEST"]
      ))
      result = dw.fetchall()
      self.assertTrue(result[0][0] == 'test')

  def test_update_2(self):
      dw.execute(sql.update(
        "LKP_TEST",
        ["TEST_ID"],
        [3],
        where = ["TEST_ID = 1"]
      ))
      dw_con.commit()
      dw.execute(sql.select(
        ["TEST_ID"],
        ["LKP_TEST"]
      ))
      result = dw.fetchall()
      self.assertTrue(result[0][0] == 3)

  def test_update_3(self):
    temporary = "(" + sql.select(
              ["TEST_ID id", "TEST_DESC descr"],
              ["LKP_TEST"],
              where = ["TEST_ID = 3"]
              ) + ") A"
    dw.execute(sql.update(
        "LKP_TEST",
        ["TEST_ID"],
        [1],
        temporary,
        where = ["TEST_ID = A.id"]
      ))

    dw.execute(sql.select(
        ["TEST_ID"],
        ["LKP_TEST"]
      ))
    result = dw.fetchall()
    self.assertTrue(result[0][0] == 1)


if __name__ == '__main__':
    unittest.main()