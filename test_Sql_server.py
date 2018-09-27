
import pymssql
from Sql_server import Sql_server
from DMS import DMS_TYPE

sql = Sql_server(DMS_TYPE.SQL_SERVER)
results1 = []
results2 = []
results3 = []


def select_simple_1():
    results1.append(sql.select(
        ["FUNCIONARIO_ID", "FUNCIONARIO_NOMBRE", "FUNCIONARIO_APELLIDO"],
        ["LKP_FUNCIONARIO"]
    ))


def select_simple_2():
    results1.append(sql.select(
        ["FUNCIONARIO_ID", "FUNCIONARIO_NOMBRE", "FUNCIONARIO_APELLIDO"],
        ["LKP_FUNCIONARIO"],
        where=["FUNCIONARIO_LOGIN = 'NURRUTY'"]
    ))


def select_simple_2_1():
    results1.append(sql.select(
        ["FUNCIONARIO_ID", "FUNCIONARIO_NOMBRE", "FUNCIONARIO_APELLIDO"],
        ["LKP_FUNCIONARIO"],
        where=[("FUNCIONARIO_LOGIN = 'NURRUTY' OR FUNCIONARIO_APELLIDO = 'URRUTY'")]
    ))


def select_simple_3():
    results1.append(sql.select(
        ["FUNCIONARIO_ID", "FUNCIONARIO_NOMBRE", "FUNCIONARIO_APELLIDO"],
        ["LKP_FUNCIONARIO"],
        order=["FUNCIONARIO_LOGIN"]
    ))


def select_simple_3_2():
    results1.append(sql.select(
        ["FUNCIONARIO_ID", "FUNCIONARIO_NOMBRE", "FUNCIONARIO_APELLIDO"],
        ["LKP_FUNCIONARIO"],
        order=["FUNCIONARIO_LOGIN", "FUNCIONARIO_ID"]
    ))


def select_simple_4():
    results1.append(sql.select(
        ["FUNCIONARIO_ID", "FUNCIONARIO_NOMBRE", "FUNCIONARIO_APELLIDO"],
        ["LKP_FUNCIONARIO"],
        where=["FUNCIONARIO_LOGIN = NURRUTY"],
        order=["FUNCIONARIO_ID"]
    ))


def select_simple_5():
    results1.append(sql.select(
        ["max(FUNCIONARIO_ID)", "FUNCIONARIO_APELLIDO"],
        ["LKP_FUNCIONARIO"],
        where=["FUNCIONARIO_LOGIN = NURRUTY"],
        group=["FUNCIONARIO_APELLIDO"]
    ))


print('Test Select simple 1')
select_simple_1()
print('Test Select simple 2.1')
select_simple_2()
print('Test Select simple 2.1')
select_simple_2_1()
print('Test Select simple 3.1')
select_simple_3()
print('Test Select simple 3.2')
select_simple_3_2()
print('Test Select simple 4')
select_simple_4()
print('Test Select simple 5')
select_simple_5()

for i, r in enumerate(results1):
    print(i)
    print(r)


def select_join_1():
    results2.append(sql.select(
        ["FUNCIONARIO_ID", "o.ORGANIGRAMA_ID"],
        ["LKP_FUNCIONARIO f", "LKP_ORGANIGRAMA o"],
        join_types=["JOIN"],
        join_conditions=[["f.ORGANIGRAMA_ID = o.ORGANIGRAMA_ID"]]
    ))


def select_join_1_1():
    results2.append(sql.select(
        ["FUNCIONARIO_ID", "o.ORGANIGRAMA_ID", "c.CARGO_DESC"],
        ["LKP_FUNCIONARIO f", "LKP_ORGANIGRAMA o", "LKP_CARGO c"],
        join_types=["JOIN", "JOIN"],
        join_conditions=[["f.ORGANIGRAMA_ID = o.ORGANIGRAMA_ID"], [
            "f.CARGO_ID = c.CARGO_ID"]]
    ))


def select_join_1_2():
    temporary = "(" + results1[0] + ") f"
    results2.append(sql.select(
        ["T.FUNCIONARIO_ID", "o.ORGANIGRAMA_ID", "c.CARGO_DESC"],
        [temporary, "LKP_ORGANIGRAMA o", "LKP_CARGO c"],
        join_types=["JOIN", "JOIN"],
        join_conditions=[["f.ORGANIGRAMA_ID = o.ORGANIGRAMA_ID"], [
            "f.CARGO_ID = c.CARGO_ID"]]
    ))


def select_join_2():
    results2.append(sql.select(
        ["FUNCIONARIO_ID", "o.ORGANIGRAMA_ID"],
        ["LKP_FUNCIONARIO f", "LKP_ORGANIGRAMA o"],
        join_types=["JOIN"],
        join_conditions=[["f.ORGANIGRAMA_ID = o.ORGANIGRAMA_ID"]],
        where=["f.FUNCIONARIO_NOMBRE like 'NICOLAS"]
    ))


def select_join_2_1():
    results2.append(sql.select(
        ["FUNCIONARIO_ID", "o.ORGANIGRAMA_ID"],
        ["LKP_FUNCIONARIO f", "LKP_ORGANIGRAMA o"],
        join_types=["JOIN"],
        join_conditions=[["f.ORGANIGRAMA_ID = o.ORGANIGRAMA_ID"]],
        where=["f.FUNCIONARIO_NOMBRE like 'NICOLAS OR o.DEPARTAMENTO_ID = 3","length(f.FUNCIONARIO_LOGIN) > 0"]
    ))

def select_join_3():
    temporary = "(" + results1[0] + ") f"
    results2.append(sql.select(
        ["T.FUNCIONARIO_ID", "o.ORGANIGRAMA_ID", "c.CARGO_DESC"],
        [temporary, "LKP_ORGANIGRAMA o", "LKP_CARGO c"],
        join_types=["JOIN", "JOIN"],
        join_conditions=[["f.ORGANIGRAMA_ID = o.ORGANIGRAMA_ID"], [
            "f.CARGO_ID = c.CARGO_ID"]],
        order= ["f.FUNCIONARIO_ID","c.CARGO_ID"]
    ))

def select_join_4():
    temporary = "(" + results1[0] + ") f"
    results2.append(sql.select(
        ["T.FUNCIONARIO_ID", "o.ORGANIGRAMA_ID", "c.CARGO_DESC"],
        [temporary, "LKP_ORGANIGRAMA o", "LKP_CARGO c"],
        join_types=["JOIN", "JOIN"],
        join_conditions=[["f.ORGANIGRAMA_ID = o.ORGANIGRAMA_ID"], [
            "f.CARGO_ID = c.CARGO_ID"]],
        where=["f.FUNCIONARIO_NOMBRE like 'NICOLAS OR o.DEPARTAMENTO_ID = 3","length(f.FUNCIONARIO_LOGIN) > 0"],
        order= ["f.FUNCIONARIO_ID","c.CARGO_ID"]
    ))


def select_join_5():
    temporary = "(" + results1[0] + ") f"
    results2.append(sql.select(
        ["max(f.FUNCIONARIO_ID)", "o.ORGANIGRAMA_ID", "c.CARGO_DESC"],
        [temporary, "LKP_ORGANIGRAMA o", "LKP_CARGO c"],
        join_types=["JOIN", "JOIN"],
        join_conditions=[["f.ORGANIGRAMA_ID = o.ORGANIGRAMA_ID"], [
            "f.CARGO_ID = c.CARGO_ID"]],
        where=["f.FUNCIONARIO_NOMBRE like 'NICOLAS OR o.DEPARTAMENTO_ID = 3","length(f.FUNCIONARIO_LOGIN) > 0"],
        group= ["o.ORGANIGRAMA_ID", "c.CARGO_DESC"]
    ))

print('Test Select simple 1')
select_join_1()
print('Test Select simple 1.1')
select_join_1_1()
print('Test Select simple 1.2')
select_join_1_2()
print('Test Select simple 2')
select_join_2()
print('Test Select simple 2.1')
select_join_2_1()
print('Test Select simple 3')
select_join_3()
print('Test Select simple 4')
select_join_4()
print('Test Select simple 5')
select_join_5()

for i, r in enumerate(results2):
    print(i)
    print(r)


def insert_1():
    results3.append(sql.insert(
        ["TEST"],
        ["TEST_ID","TEST_DESC"],
        "SELECT 1, 'prueba'"
    ))

def insert_2():
    temporary = "(\n" + results1[0] + "\n)"
    results3.append(sql.insert(
        ["TEST"],
        ["TEST_ID","TEST_DESC"],
        temporary
    ))

def insert_3():
    temporary1 = "(\n" + results1[0] + "\n) A"
    temporary2 = "(\n" + results2[0] + "\n) B"
    temporary = sql.select(
        ["A.FUNCIONARIO_ID,B.*"],
        [temporary1,temporary2],
        join_types=["JOIN"],
        join_conditions=[["A.FUNCIONARIO_ID = B.FUNCIONARIO_ID"]]
    )
    results3.append(sql.insert(
        ["TEST"],
        ["TEST_ID","TEST_DESC"],
        temporary
    ))


print('Test insert 1')
insert_1()
print('Test insert 2')
insert_2()
print('Test insert 3')
insert_3()

for i, r in enumerate(results3):
    print(i)
    print(r)