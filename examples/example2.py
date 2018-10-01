
etl = ETL(cursor,"sp_LKP_FUNCIONARIO")

lkp = SCDimension.fromdb(cursor,"LKP_FUNCIONARIO")

usuariosbolt = Table.fromdb(cursor,"USUARIOS")
usuarioscore = Table.fromdb(cursor,"USUARIOS")

temp = Query(columns=[Column("1","numeric(1)"),Column("'DESCONOCIDO","varchar()")])


if lkp.isEmpty():
    etl.add(lkp.insert(
        source = temp.code()
    ))

query2 = Query(
    
    columns = [usuariosbolt.columns["CARGO"],usuariosbolt.clumns["ORGANIGRAMA"]]
    tables = [usuariosbolt,usuarioscore]
    join_type = ["JOIN"]
    join_conditions = [(usuariosbolt.columns["LOGIN"],usuarioscore.columns["LOGIN"])]
    where = [ sql.utils.isTrue(usuariosbolt.columns["ACTIVO")] , sql.utils.isGrater(usuariosbolt.columns["ID"], 50)]

)

temporal = Table.fromquery(cursor,query2) 

etl.add(lkp.update_scd2(
    source = temporal # las columnas de la tempo deben coincidir al target
    update_columns = [lkp["ORGANIGRAMA_ID"],lkp["CARGO_ID"]] #por defecto todas
))

etl.add(lkp.update(
    values = [ lkp["FUNCIONARIO_NOM"], lkp["FUNCIONARIO_APELLIDO"],
    source = temporal,
    source_columns = [temporal["FUNCIONARIO_NOM","FUNCIONARIO_APE"]]
)

etl.create()