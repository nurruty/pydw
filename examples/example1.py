
dw_con = connect("DW")
dw = dw_con.cursor()

stg_con = connect("STG")
stg = stg_con.cursor()

ods_con = connect("ODS")
ods = ods_con.cursor()

etl = ETL(stg,"sp_LKP_CAUSAL_BAJA")

lkp = SCDimension1(dw,"LKP_CAUSAL_BAJA")

source = Table.fromdb(
    ods,
    columns = ["CAUSAL_BAJA_ID" , "CAUSAL_BAJA_DESC"]
    )

etl.update_scd1(
    target = lkp,
    source = source
)

etl.create() #podria ser etl.execute() y en vez de generar el sp ejecuta todo

