#!/usr/bin/python

import pymssql
from Table import Table
from Query import Query
from SCDimension2 import SCDimension2
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

usuarios_bolt = Table.from_db(dbms, ods, "Dimensiones_ODS", "dbo", "RAFAP52_USUARIO", alias='usuabolt')
usuarios_core = Table.from_db(dbms, ods, "Dimensiones_ODS", "dbo", "RAFAP92_USUARIO", alias='usuacore')
lkp_organigrama = Table.from_db(dbms, dw, "Dimensiones_DW", "dbo", "LKP_ORGANIGRAMA", alias='org')
lkp_cargo = Table.from_db(dbms, dw, "Dimensiones_DW", "dbo", "LKP_CARGO", alias='carg')

lkp = SCDimension2.from_db(dbms, dw, "Dimensiones_DW", "dbo", "TEST_LKP_FUNCIONARIO",
                           valid_column_name ='REGISTROVALIDO',
                           init_column_name = 'REGISTRO_FECHA_INICIO',
                           end_column_name = 'REGISTRO_FECHA_FIN',
                           alias="lkp")

def test_1_create_dimension():
    return True

def test_2_update_dimension_1():

    lkp.truncate()



    query1 = Query(
                dbms = dbms,
                sources=[usuarios_bolt,lkp_organigrama,lkp_cargo],
                columns=[
                  usuarios_bolt.columns["UsuarioFuncionarioNumero"],
                  usuarios_bolt.columns["UsuarioDocumento"],
                  usuarios_bolt.columns["UsuarioLogin"],
                  lkp_organigrama.columns["ORGANIGRAMA_ID"],
                  lkp_cargo.columns["CARGO_ID"],
                  usuarios_bolt.columns["CargoId"]
                ],
                join_types=["JOIN","JOIN"],
                join_conditions=[
                    [
                    usuarios_bolt.columns["UsuarioDeptoId"].equals(lkp_organigrama.columns["DEPARTAMENTO_COD"]),
                    usuarios_bolt.columns["UsuarioDivisionId"].equals(lkp_organigrama.columns["DIVISION_COD"]),
                    usuarios_bolt.columns["UsuarioDivisionId"].equals(lkp_organigrama.columns["SECTOR_COD"])
                    ],
                    [
                    usuarios_bolt.columns["CargoId"].equals(lkp_cargo.columns["CARGO_COD"])
                    ]
                  ],
                alias = 'hoy'
            )

    return lkp.update_scd2(
            source = query1,
            join_conditions = [["lkp.FUNCIONARIO_NUM = hoy.UsuarioFuncionarioNumero"]],
            audited_columns = [
                lkp.columns["CARGO_ID"],
                lkp.columns["ORGANIGRAMA_ID"]
            ]
        )

# def test_2_update_dimension_2():

#   return lkp.update_scd1(
#           source = table,
#           join_conditions = [["CauBajCod = TEST_ID"]]
#         )

# def test2_update_dimension_3():

#   temp = Query(dbms=dbms, columns=[Column("1","numeric(1)"),Column("'DESCONOCIDO'","varchar()")])
#   cond = Query(
#     dbms=dbms,
#     sources=[lkp],
#     columns=[Column(dbms.count("TEST_ID"), "numeric(3)")]
#   )

#   statements = "IF (" + cond.code()  + ") = 0 BEGIN\n"
#   statements +=  lkp.insert(
#       query= temp
#   )
#   statements += "END\n"

#   statements += lkp.update_scd1(
#     source = table,
#     join_conditions = [["CauBajCod = TEST_ID"]]

#   )

#   statements += lkp.update_from(
#     columns = [lkp.columns["TEST_DESC"]],
#     source = table,
#     where = [table.columns["CauBajCod"].equals(lkp.columns["TEST_ID"])]
#     )
#   return statements






results.append(test_2_update_dimension_1())
#results.append(test_2_update_dimension_2())
#results.append(test2_update_dimension_3())

for i,result in enumerate(results):
  print("*************************************************************")
  print("*************************************************************")
  print(result)
  print("*************************************************************")
  print("*************************************************************")
