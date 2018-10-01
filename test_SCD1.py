#!/usr/bin/python

import pymssql
from Table import Table
from Query import Query
from SCDimension1 import SCDimension1
from DBMS import DBMS_TYPE


dw_con = pymssql.connect(server='rafap62', user='iadev', password='Pdtyynqsn2018_', database='Dimensiones_DW')
dw_ods = pymssql.connect(server='rafap62', user='iadev', password='Pdtyynqsn2018_', database='Dimensiones_ODS')

dw = dw_con.cursor()
ods = dw_ods.cursor()
tdms = DBMS_TYPE.SQL_SERVER

results = []

def test_1_create_dimension():
    return True

def test_2_update_dimension_1():
    table = Table.from_db(tdms, ods, "Dimensiones_ODS", "dbo", "RAFAP51_CAUBAJ")
    lkp = SCDimension1.from_db(tdms, dw, "Dimensiones_DW", "dbo", "LKP_TEST")

    query = Query(
                sources=[table],
                columns=[table["CauBajCod"],table["CauBajDesc"]]
                alias = 'cbaja'
            )

    return lkp.update_scd1(
            tdms,
            query.code(),
            [["CauBajCod = TEST_ID"]]
        )


''' def test_2_update_dimension_2():

  table = Table.fromTable(tdms,ods,"Dimensiones_ODS","RAFAP51_CAUBAJ")
  lkp = SCDimension1.fromTable(tdms,dw,"Dimensiones_DW","LKP_TEST")

  return lkp.update_scd1(
          values = lkp.columns[:1],
          source = [table],
          source_values = table.columns[:1],
          join_conditions = [["CauBajCod = TEST_ID"]]
        )

table = SCDimension1.fromTable(tdms,dw,"Dimensiones_ODS","RAFAP51_CAUBAJ")
lkp = SCDimension1.fromTable(tdms,dw,"Dimensiones_DW","LKP_TEST")

def test2_update_dimension_3(lookup_table,data_table,empty_row,
  insert_values, update_values):

  att = ",".join(empty_row)
  temp = Table.fromTextQuery(tdms,'',"SELECT {0} \n".format(att))

  statements = "IF BEGIN (" + lkp.aggregate(operations = ["COUNT"], operation_values = ['*']) + ") = 0 \n"
  statements +=  lkp.insert(
      values = insert_values,
      source= temp
      )
  statements += "END\n"

  statements += lkp.update_scd1(
    source= [table],
    source_values= table.columns
    ) '''

  #statements += lkp.update(["TEST_DESC"],[table.columns[1]],table)
  #return statements






#results.append(test2_update_dimension_1())
results.append(test_2_update_dimension_2())
#Sresults.append(test2_update_dimension_3(lkp,table))

for i,result in enumerate(results):
  print("******************")
  print(result)
