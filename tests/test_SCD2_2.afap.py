#!/usr/bin/python

import pymssql
from dwobjects import Table, Query, SCDimension2, Column
from dbms import DBMS_TYPE, SQLServer
from copy import deepcopy
from Etl import Etl


dw_con = pymssql.connect(server='rafap62', user='iadev', password='Pdtyynqsn2018_', database='Dimensiones_DW')
dw_stg = pymssql.connect(server='rafap62', user='iadev', password='Pdtyynqsn2018_', database='Dimensiones_STG')
dw_ods = pymssql.connect(server='rafap62', user='iadev', password='Pdtyynqsn2018_', database='Dimensiones_ODS')

dw = dw_con.cursor()
ods = dw_ods.cursor()
stg = dw_stg.cursor()
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
                           nk_column_names = ['FUNCIONARIO_CEDULA'],
                           alias="lkp")

def test_1_create_dimension():
    return True

def test_2_update_dimension_1():

    code = ''

    code += lkp.truncate()

    (aux_code,today_table) = lkp.create_temporal(
                                table_name = 'hoy',
                                not_column_names= ['FUNCIONARIO_ID']
                            )

    code += aux_code

    query1 = Query(
                dbms = dbms,
                sources=[usuarios_bolt,lkp_organigrama,lkp_cargo],
                columns=[
                  usuarios_bolt.columns["UsuarioFuncionarioNumero"],
                  usuarios_bolt.columns["UsuarioDocumento"],
                  usuarios_bolt.columns["UsuarioLogin"],
                  lkp_organigrama.columns["ORGANIGRAMA_ID"],
                  lkp_cargo.columns["CARGO_ID"]
                ],
                join_types=["JOIN","JOIN"],
                join_conditions=[
                    [
                    usuarios_bolt.columns["UsuarioDeptoId"].equals(lkp_organigrama.columns["DEPARTAMENTO_COD"]),
                    usuarios_bolt.columns["UsuarioDivisionId"].equals(lkp_organigrama.columns["DIVISION_COD"]),
                    usuarios_bolt.columns["UsuarioSectorId"].equals(lkp_organigrama.columns["SECTOR_COD"])
                    ],
                    [
                    usuarios_bolt.columns["CargoId"].equals(lkp_cargo.columns["CARGO_COD"])
                    ]
                  ],
                alias = 'hoy'
            )

    code += today_table.insert(
        query = query1,
        columns = [
            lkp.columns["FUNCIONARIO_NUM"],
            lkp.columns["FUNCIONARIO_CEDULA"],
            lkp.columns["FUNCIONARIO_LOGIN"],
            lkp.columns["ORGANIGRAMA_ID"],
            lkp.columns["CARGO_ID"]
        ]
    )


    code += today_table.update(
        columns = [
            lkp.columns["LOCALIDAD_ID"],
            lkp.columns["DEPARTAMENTO_ID"],
            lkp.columns["MUTUALISTA_ID"],
            lkp.columns["CAUSAL_BAJA_ID"],
        ],
        data = [str(0), str(0), str(0), str(0),]
    )

    query2 = Query(
        dbms = dbms,
        sources=[usuarios_bolt, usuarios_core],
        columns=[
            usuarios_bolt.columns["UsuarioLogin"],
            usuarios_core.columns["GAMUsuarioId"]
        ],
        join_types=["JOIN"],
        join_conditions=[[usuarios_bolt.columns["UsuarioLogin"].equals(usuarios_core.columns["GAMUsuarioLogin"])]],
        where=[
            usuarios_bolt.columns["UsuarioLogin"].different("''"),
            usuarios_core.columns["GAMUsuarioBorrado"].equals(0)]
    )


    code += today_table.update_from_query(
        columns= [today_table.columns["FUNCIONARIO_HASH_CRM"]],
        source = query2,
        source_columns = [query2.columns["GAMUsuarioId"]],
        where = [today_table.columns["FUNCIONARIO_LOGIN"].equals(query2.columns["UsuarioLogin"],False)]

    )

    code+= lkp.update_scd2(
            source = today_table,
            join_key = [today_table.columns["FUNCIONARIO_NUM"]],
            audited_columns = [
                lkp.columns["CARGO_ID"],
                lkp.columns["ORGANIGRAMA_ID"]
            ]
        )
    #print(code)
    etl = Etl(stg, dbms, 'Dimensiones_STG', 'dbo', 'sp_TEST_LKP_FUNCIONARIO', code = code)
    text_file = open("Output.sql", "w")
    text_file.write(etl.create())
    text_file.close()

# # def test_2_update_dimension_2():

# #   return lkp.update_scd1(
# #           source = table,
# #           join_conditions = [["CauBajCod = TEST_ID"]]
# #         )

# # def test2_update_dimension_3():

# #   temp = Query(dbms=dbms, columns=[Column("1","numeric(1)"),Column("'DESCONOCIDO'","varchar()")])
# #   cond = Query(
# #     dbms=dbms,
# #     sources=[lkp],
# #     columns=[Column(dbms.count("TEST_ID"), "numeric(3)")]
# #   )

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






# results.append(test_2_update_dimension_1())
# #results.append(test_2_update_dimension_2())
# #results.append(test2_update_dimension_3())

# for i,result in enumerate(results):
#   print("*************************************************************")
#   print("*************************************************************")
#   print(result)
#   print("*************************************************************")
#   print("*************************************************************")
