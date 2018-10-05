#!/usr/bin/python

import pymssql
from Table import Table
from Query import Query
from SCDimension1 import SCDimension1
from DBMS import DBMS_TYPE
from SQLServer import SQLServer
from Column import Column


dw_con = pymssql.connect(server='rafap62', user='iadev', password='Pdtyynqsn2018_', database='Dimensiones_DW')
dw_ods = pymssql.connect(server='rafap62', user='iadev', password='Pdtyynqsn2018_', database='Dimensiones_ODS')

dw = dw_con.cursor()
ods = dw_ods.cursor()
tdms = DBMS_TYPE.SQL_SERVER
dbms = SQLServer()

results = []

table = Table.from_db(dbms, ods, "Dimensiones_ODS", "dbo", "RAFAP51_CAUBAJ",alias='caubaj')
lkp = SCDimension1.from_db(dbms, dw, "Dimensiones_DW", "dbo", "LKP_TEST", alias="lkp")

def test_1_create_dimension():
    return True

def test_2_update_dimension_1():

    query = Query(
                dbms = dbms,
                sources=[table],
                columns=[table.columns["CauBajCod"],table.columns["CauBajDsc"]],
                alias = 'cbaja'
            )

    return lkp.update_scd1(
            source = query,
            join_conditions = [["CauBajCod = TEST_ID"]]
        )

def test_2_update_dimension_2():

  return lkp.update_scd1(
          source = table,
          join_conditions = [["CauBajCod = TEST_ID"]]
        )

def test2_update_dimension_3():

  temp = Query(dbms=dbms, columns=[Column("1","numeric(1)"),Column("'DESCONOCIDO'","varchar()")])
  cond = Query(
    dbms=dbms,
    sources=[lkp],
    columns=[Column(dbms.count("TEST_ID"), "numeric(3)")]
  )

  statements = "IF (" + cond.code()  + ") = 0 BEGIN\n"
  statements +=  lkp.insert(
      query= temp
  )
  statements += "END\n"

  statements += lkp.update_scd1(
    source = table,
    join_conditions = [["CauBajCod = TEST_ID"]]

  )

  statements += lkp.update_from(
    columns = [lkp.columns["TEST_DESC"]],
    source = table,
    where = [table.columns["CauBajCod"].equals(lkp.columns["TEST_ID"])]
    )
  return statements






results.append(test_2_update_dimension_1())
results.append(test_2_update_dimension_2())
results.append(test2_update_dimension_3())

for i,result in enumerate(results):
  print("******************")
  print(result)
