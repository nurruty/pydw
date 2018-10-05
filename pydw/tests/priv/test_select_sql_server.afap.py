import unittest
import pymssql
from Sql_server import Sql_server
from DMS import DMS_TYPE

dw_con = pymssql.connect(server='rafap62', user='iadev', password='Pdtyynqsn2018_', database='Dimensiones_DW')
dw = dw_con.cursor()

sql = Sql_server(DMS_TYPE.SQL_SERVER)


class TestSQLServer(unittest.TestCase):
  def test_select_simple_1(self):
    dw.execute(sql.select(
        ["FUNCIONARIO_ID", "FUNCIONARIO_NOM1", "FUNCIONARIO_APE1"],
        ["LKP_FUNCIONARIO"]
    ))
    self.assertTrue(len(dw.fetchall()[0]) > 1)

  def test_select_simple_2(self):
    dw.execute(sql.select(
        ["FUNCIONARIO_ID", "FUNCIONARIO_NOM1", "FUNCIONARIO_APE1"],
        ["LKP_FUNCIONARIO"],
        where=["FUNCIONARIO_LOGIN = 'NURRUTY'"]
    ))
    self.assertTrue(len(dw.fetchall()[0]) > 1)


  def test_select_simple_2_1(self):
      dw.execute(sql.select(
          ["FUNCIONARIO_ID", "FUNCIONARIO_NOM1", "FUNCIONARIO_APE1"],
          ["LKP_FUNCIONARIO"],
          where=[("FUNCIONARIO_LOGIN = 'NURRUTY' OR FUNCIONARIO_APE1 = 'URRUTY'")]
      ))
      self.assertTrue(len(dw.fetchall()[0]) > 1)


  def test_select_simple_3(self):
      dw.execute(sql.select(
          ["FUNCIONARIO_ID", "FUNCIONARIO_NOM1", "FUNCIONARIO_APE1"],
          ["LKP_FUNCIONARIO"],
          order=["FUNCIONARIO_LOGIN"]
      ))
      self.assertTrue(len(dw.fetchall()[0]) > 1)


  def test_select_simple_3_2(self):
      dw.execute(sql.select(
          ["FUNCIONARIO_ID", "FUNCIONARIO_NOM1", "FUNCIONARIO_APE1"],
          ["LKP_FUNCIONARIO"],
          order=["FUNCIONARIO_LOGIN", "FUNCIONARIO_ID"]
      ))
      self.assertTrue(len(dw.fetchall()[0]) > 1)


  def test_select_simple_4(self):
      dw.execute(sql.select(
          ["FUNCIONARIO_ID", "FUNCIONARIO_NOM1", "FUNCIONARIO_APE1"],
          ["LKP_FUNCIONARIO"],
          where=["FUNCIONARIO_LOGIN = 'NURRUTY'"],
          order=["FUNCIONARIO_ID"]
      ))
      self.assertTrue(len(dw.fetchall()[0]) > 1)


  def test_select_simple_5(self):
      dw.execute(sql.select(
          ["max(FUNCIONARIO_ID)", "FUNCIONARIO_APE1"],
          ["LKP_FUNCIONARIO"],
          where=["FUNCIONARIO_LOGIN = 'NURRUTY'"],
          group=["FUNCIONARIO_APE1"]
      ))
      self.assertTrue(len(dw.fetchall()[0]) > 1)


if __name__ == '__main__':
    unittest.main()