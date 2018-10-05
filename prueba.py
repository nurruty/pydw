from pydw.dbms import SQLServer
from pydw.connections import sql_server
from pydw.etl.steps import (LoadTable, SelectValues, JoinTables, UpdateColumns,
SCDimension2Load, SCDimension2Update)



ods = sql_server(
              server = 'rafap62',
              user = 'iadev',
              password = 'Pdtyynqsn2018_',
              database = 'Dimensiones_ODS'
)

dw = sql_server(
              server = 'rafap62',
              user = 'iadev',
              password = 'Pdtyynqsn2018_',
              database = 'Dimensiones_DW'
)


dbms = SQLServer()

input_ = []
data = dict()
tables = dict()

# Upload BoltUsuarios
data = dict()
data['database'] = 'Dimensiones_ODS'
data['schema'] =  'dbo'
data['table_name'] = 'RAFAP52_USUARIO'
step1 = LoadTable(dbms, 'usuariosbolt', [ods], data)
tables['usuariosbolt'] = step1.output()['table']

# Upload CoreUsuarios
data['database'] = 'Dimensiones_ODS'
data['schema'] =  'dbo'
data['table_name'] = 'RAFAP92_USUARIO'
step1_1 = LoadTable(dbms, 'usuarioscore', [ods], data)
tables['usuarioscore'] = step1_1.output()['table']

# Upload LKP_Organigrama
data['database'] = 'Dimensiones_DW'
data['schema'] =  'dbo'
data['table_name'] = 'LKP_ORGANIGRAMA'
step1_2 = LoadTable(dbms, 'lkporg', [dw], data)
tables['lkporg'] = step1_2.output()['table']

# Upload LKP_Cargo
data['database'] = 'Dimensiones_DW'
data['schema'] =  'dbo'
data['table_name'] = 'LKP_CARGO'
step1_3 = LoadTable(dbms, 'lkpcargo', [dw], data)
tables['lkpcargo'] = step1_3.output()['table']

# Upload LKP_FUNCIONARIO
data['database'] = 'Dimensiones_DW'
data['schema'] =  'dbo'
data['dimension_name'] = 'TEST_LKP_FUNCIONARIO'
data['valid_column'] = 'REGISTROVALIDO'
data['init_column'] = 'REGISTRO_FECHA_INICIO'
data['end_column'] = 'REGISTRO_FECHA_FIN'
data['nk_column_names'] = ['FUNCIONARIO_CEDULA']
step1_4 = SCDimension2Load(dbms, 'lkpfuncionario', [dw], data)
tables['lkpfuncio'] = step1_4.output()['dimension']


# Join BoltUsuarios y LKP_Organigrama
input_tables = [tables['usuariosbolt'], tables['lkporg']]
data['columns_table1'] = ['UsuarioFuncionarioNumero','UsuarioDocumento','UsuarioLogin','CargoId']
data['columns_table2'] = ['ORGANIGRAMA_ID']
data['join'] = "JOIN"
data['key_table1'] = ['UsuarioDeptoId','UsuarioDivisionId','UsuarioSectorId']
data['key_table2'] = ['DEPARTAMENTO_COD', 'DIVISION_COD', 'SECTOR_COD']
step2 = JoinTables(dbms, 'Join1', input_tables, data)
result = step2.code()


# Join Step2 y LKP_CARGO
input_tables = [step2.output()['table'], tables['lkpcargo']]
data['columns_table1'] = ['UsuarioFuncionarioNumero','UsuarioDocumento','UsuarioLogin','ORGANIGRAMA_ID']
data['columns_table2'] = ['CARGO_ID']
data['join'] = "JOIN"
data['key_table1'] = ['CargoId']
data['key_table2'] = ['CARGO_ID']
step3 = JoinTables(dbms, 'Join2', input_tables, data)
result += step3.code()



#Update LKP_FUNCIONARIO
input_tables = [step3.output()['table'], tables['lkpfuncio']]
data['incoming_columns'] = ['FUNCIONARIO_NUM', 'FUNCIONARIO_CEDULA', 'FUNCIONARIO_LOGIN', 'ORGANIGRAMA_ID', 'CARGO_ID']
data['natural_key'] = ['FUNCIONARIO_CEDULA']
data['audited_columns'] = ['ORGANIGRAMA_ID', 'CARGO_ID']
step4 = SCDimension2Update(dbms, 'UpdateLkpFuncio', input_tables, data)
result += step4.code()

text_file = open("Output.sql", "w")
text_file.write(result)
text_file.close()
