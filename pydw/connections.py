import pymssql


def sql_server(server, user, password, database):

    conn = pymssql.connect(
    server= server,
    user= user,
    password= password,
    database= database
    )

    cursor = conn.cursor()

    return cursor
