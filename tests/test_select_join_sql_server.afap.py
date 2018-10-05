import unittest
import pymssql
from Sql_server import Sql_server
from DMS import DMS_TYPE

dw_con = pymssql.connect(server='rafap62', user='iadev', password='Pdtyynqsn2018_', database='Dimensiones_DW')
dw = dw_con.cursor()

sql = Sql_server(DMS_TYPE.SQL_SERVER)


class TestSQLServer(unittest.TestCase):
  def test_select_join_1(self):
    dw.execute(sql.select(
        ["FUNCIONARIO_ID", "o.ORGANIGRAMA_ID"],
        ["LKP_FUNCIONARIO f", "LKP_ORGANIGRAMA o"],
        join_types=["JOIN"],
        join_conditions=[["f.ORGANIGRAMA_ID = o.ORGANIGRAMA_ID"]]
    ))
    self.assertTrue(len(dw.fetchall()[0]) > 1)


  def test_select_join_1_1(self):
    dw.execute(sql.select(
        ["FUNCIONARIO_ID", "o.ORGANIGRAMA_ID", "c.CARGO_DESC"],
        ["LKP_FUNCIONARIO f", "LKP_ORGANIGRAMA o", "LKP_CARGO c"],
        join_types=["JOIN", "JOIN"],
        join_conditions=[["f.ORGANIGRAMA_ID = o.ORGANIGRAMA_ID"], [
            "f.CARGO_ID = c.CARGO_ID"]]
    ))
    self.assertTrue(len(dw.fetchall()[0]) > 1)


  def test_select_join_1_2(self):
    temporary = "(" + sql.select(
              ["FUNCIONARIO_ID", "ORGANIGRAMA_ID", "CARGO_ID"],
              ["LKP_FUNCIONARIO"]
              ) + ") f"
    dw.execute(sql.select(
        ["f.FUNCIONARIO_ID", "o.ORGANIGRAMA_ID", "c.CARGO_DESC"],
        [temporary, "LKP_ORGANIGRAMA o", "LKP_CARGO c"],
        join_types=["JOIN", "JOIN"],
        join_conditions=[["f.ORGANIGRAMA_ID = o.ORGANIGRAMA_ID"], [
            "f.CARGO_ID = c.CARGO_ID"]]
    ))
    self.assertTrue(len(dw.fetchall()[0]) > 1)


  def test_select_join_2(self):
    dw.execute(sql.select(
        ["FUNCIONARIO_ID", "o.ORGANIGRAMA_ID"],
        ["LKP_FUNCIONARIO f", "LKP_ORGANIGRAMA o"],
        join_types=["JOIN"],
        join_conditions=[["f.ORGANIGRAMA_ID = o.ORGANIGRAMA_ID"]],
        where=["f.FUNCIONARIO_NOM1 like 'NICOLAS' "]
    ))
    self.assertTrue(len(dw.fetchall()[0]) > 1)


  def test_select_join_2_1(self):
    dw.execute(sql.select(
        ["FUNCIONARIO_ID", "o.ORGANIGRAMA_ID"],
        ["LKP_FUNCIONARIO f", "LKP_ORGANIGRAMA o"],
        join_types=["JOIN"],
        join_conditions=[["f.ORGANIGRAMA_ID = o.ORGANIGRAMA_ID"]],
        where=["f.FUNCIONARIO_NOM1 like 'NICOLAS' OR o.DEPARTAMENTO_COD = 3","len(f.FUNCIONARIO_LOGIN) > 0"]
    ))
    self.assertTrue(len(dw.fetchall()[0]) > 1)

  def test_select_join_3(self):
    temporary = "(" + sql.select(
              ["FUNCIONARIO_ID", "ORGANIGRAMA_ID", "CARGO_ID"],
              ["LKP_FUNCIONARIO"]
              ) + ") f"
    dw.execute(sql.select(
        ["f.FUNCIONARIO_ID", "o.ORGANIGRAMA_ID", "c.CARGO_DESC"],
        [temporary, "LKP_ORGANIGRAMA o", "LKP_CARGO c"],
        join_types=["JOIN", "JOIN"],
        join_conditions=[["f.ORGANIGRAMA_ID = o.ORGANIGRAMA_ID"], [
            "f.CARGO_ID = c.CARGO_ID"]],
        order= ["f.FUNCIONARIO_ID","c.CARGO_ID"]
    ))
    self.assertTrue(len(dw.fetchall()[0]) > 1)

  def test_select_join_4(self):
    temporary = "(" + sql.select(
              ["FUNCIONARIO_ID", "FUNCIONARIO_NOM1",
               "FUNCIONARIO_LOGIN", "ORGANIGRAMA_ID", "CARGO_ID"],
              ["LKP_FUNCIONARIO"]
              ) + ") f"
    dw.execute(sql.select(
        ["f.FUNCIONARIO_ID", "o.ORGANIGRAMA_ID", "c.CARGO_DESC"],
        [temporary, "LKP_ORGANIGRAMA o", "LKP_CARGO c"],
        join_types=["JOIN", "JOIN"],
        join_conditions=[["f.ORGANIGRAMA_ID = o.ORGANIGRAMA_ID"], [
            "f.CARGO_ID = c.CARGO_ID"]],
        where=["f.FUNCIONARIO_NOM1 like 'NICOLAS' OR o.DEPARTAMENTO_COD = 3","len(f.FUNCIONARIO_LOGIN) > 0"],
        order= ["f.FUNCIONARIO_ID","c.CARGO_ID"]
    ))
    self.assertTrue(len(dw.fetchall()[0]) > 1)


  def test_select_join_5(self):
    temporary = "(" + sql.select(
              ["FUNCIONARIO_ID", "FUNCIONARIO_NOM1",
               "FUNCIONARIO_LOGIN", "ORGANIGRAMA_ID", "CARGO_ID"],
              ["LKP_FUNCIONARIO"]
              ) + ") f"
    dw.execute(sql.select(
        ["max(f.FUNCIONARIO_ID)", "o.ORGANIGRAMA_ID", "c.CARGO_DESC"],
        [temporary, "LKP_ORGANIGRAMA o", "LKP_CARGO c"],
        join_types=["JOIN", "JOIN"],
        join_conditions=[["f.ORGANIGRAMA_ID = o.ORGANIGRAMA_ID"], [
            "f.CARGO_ID = c.CARGO_ID"]],
        where=["f.FUNCIONARIO_NOM1 like 'NICOLAS' OR o.DEPARTAMENTO_COD = 3","len(f.FUNCIONARIO_LOGIN) > 0"],
        group= ["o.ORGANIGRAMA_ID", "c.CARGO_DESC"]
    ))
    self.assertTrue(len(dw.fetchall()[0]) > 1)


if __name__ == '__main__':
    unittest.main()