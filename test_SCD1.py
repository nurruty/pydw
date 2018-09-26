#!/usr/bin/python

import pymssql
from Table import Table
from Dimension import SCDimension1
from DMS import DMS_TYPE


dw_con = pymssql.connect(server='rafap62', user='iadev', password='Pdtyynqsn2018_', database='Dimensiones_DW')
dw_ods = pymssql.connect(server='rafap62', user='iadev', password='Pdtyynqsn2018_', database='Dimensiones_ODS')
tdms = DMS_TYPE.SQL_SERVER

results = []

def test_1_create_dimension():
  return True

def test2_update_dimension_1():
  dw = dw_con.cursor()
  ods = dw_ods.cursor()

  source = Table.fromTable(tdms,ods,"Dimensiones_ODS","RAFAP51_CAUBAJ")
  lkp = SCDimension1.fromTable(tdms,dw,"Dimensiones_DW","LKP_CAUSAL_BAJA")

  return lkp.insert(source.columns,[source.name],[["CAUSAL_BAJA_COD = LKP_CAUSAL_COD"]])


def test2_update_dimension_2():
  return True








results.append(test2_update_dimension_1())

for result in results:
  print(result)
