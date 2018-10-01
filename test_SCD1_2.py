#!/usr/bin/python

import pymssql
from Table import Table
from Query import Query
from Column import Column
from SCDimension1 import SCDimension1
from DBMS import DBMS_TYPE
from SQLServer import SQLServer


tdms = DBMS_TYPE.SQL_SERVER
sql = SQLServer()

results = []


def test_2_update_dimension_1():
    
    column1 = Column('Name', 'Person', 'varchar(20)', False)
    column2 = Column('Age', 'Person', 'numeric(2)', False)
    table = Table(tdms, 'Person', [column1,column2], [column1.name])

    column1 = Column('Person_Name', 'Person', 'varchar(20)', False)
    column2 = Column('Person_Age', 'Person', 'numeric(2)', False)
    lkp = SCDimension1(tdms, 'Lkp_Person', [column1,column2], [column1.name])

    query = Query(
                sources=[table],
                columns=[table.columns["Name"],table.columns["Age"]],
                alias = ''
            )

    return lkp.update_scd1(
            sql,
            query,
            [[lkp.columns['Person_Name'].equals(sql,table.columns['Name'])]]
        )

def test_2_update_dimension_2():
    
    column1 = Column('Name', 'Person', 'varchar(20)', False)
    column2 = Column('Age', 'Person', 'numeric(2)', False)
    table = Table(tdms, 'Person', [column1,column2], [column1.name])

    column1 = Column('Person_Name', 'Person', 'varchar(20)', False)
    column2 = Column('Person_Age', 'Person', 'numeric(2)', False)
    lkp = SCDimension1(tdms, 'Lkp_Person', [column1,column2], [column1.name])

    return lkp.update_scd1(
            sql,
            table,
            [[lkp.columns['Person_Name'].equals(sql,table.columns['Name'])]]
        )


results.append(test_2_update_dimension_1())
results.append(test_2_update_dimension_2())

for i,result in enumerate(results):
  print("******************")
  print(result)
