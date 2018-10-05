import unittest
import pymssql
from dbms import SQLServer, DbMS_TYPE

dw_con = pymssql.connect(server='rafap62', user='iadev', password='Pdtyynqsn2018_', database='Dimensiones_DW')
dw = dw_con.cursor()

sql = Sql_server(DMS_TYPE.SQL_SERVER)

dw.execute(sql.truncate("LKP_TEST"))
dw_con.commit()

class TestSQLServer(unittest.TestCase):

  def test_insert_1(self):
      dw.execute(sql.insert(
          "LKP_TEST",
          ["TEST_ID","TEST_DESC"],
          "SELECT 1, 'prueba'"
      ))
      dw_con.commit()

  def test_insert_2(self):
      temporary = "(\n" + sql.select(
              ["FUNCIONARIO_ID", "FUNCIONARIO_NOM1"],
              ["LKP_FUNCIONARIO"],
              where=["FUNCIONARIO_LOGIN =  'NURRUTY' "]
              ) + "\n)"
      dw.execute(sql.insert(
          "LKP_TEST",
          ["TEST_ID","TEST_DESC"],
          temporary
      ))
      dw_con.commit()

  def test_insert_3(self):
      temporary1 = "(\n" + sql.select(
              ["FUNCIONARIO_ID", "FUNCIONARIO_NOM1"],
              ["LKP_FUNCIONARIO"],
              where=["FUNCIONARIO_LOGIN =  'NURRUTY' "]
              ) + "\n) A"
      temporary2 = "(\n" + sql.select(
              ["FUNCIONARIO_ID", "FUNCIONARIO_NOM1"],
              ["LKP_FUNCIONARIO"],
              where=["FUNCIONARIO_LOGIN =  'NCORREA' "]
              ) + "\n) B"
      temporary = sql.select(
          ["A.FUNCIONARIO_ID,B.FUNCIONARIO_NOM1"],
          [temporary1,temporary2],
          join_types=["JOIN"],
          join_conditions=[["A.FUNCIONARIO_ID = B.FUNCIONARIO_ID"]]
      )
      dw.execute(sql.insert(
           "LKP_TEST",
          ["TEST_ID","TEST_DESC"],
          temporary
      ))
      dw_con.commit()


if __name__ == '__main__':
    unittest.main()