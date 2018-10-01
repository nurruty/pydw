from DBMS import DBMS 

class SQLServer(DBMS):

    def create_table(self, table_name, column_names, column_types, column_nullable, key=[], foreign_keys=[]):
        statement = "CREATE TABLE {0} (\n".format(table_name)
        statement += ",\n".join(map(lambda c: "{0} {1} {2}".format(c[0], c[1], "NOT NULL" if not c[2] else "NULL"),
                                zip(column_names, column_types, column_nullable)))
        if key:
            statement += "\n CONSTRAINT [PK_{0}] PRIMARY KEY CLUSTERED ( {1} ASC )".format(table_name,",".join(key))
        statement +=  ")\n"
        return statement

    def drop_table(self, table_name):
        return "DROP TABLE {0};".format(table_name)

    def add_column(self, table_name, column, data_type):
        statement = "ALTER TABLE {0}\n".format(table_name)
        statement += "ADD {0} {1};".format(column,data_type)
        return statement

    def drop_column(self, table_name, column_name):
        statement = "ALTER TABLE {0}\n".format(table_name)
        statement += "DROP COLUMN {0};".format(column_name)
        return statement

    def alter_column(self, table_name, column_name, data_type):
        statement = "ALTER TABLE {0}\n".format(table_name)
        statement += "ALTER COLUMN {0} {1};".format(column_name,data_type)
        return statement

    def truncate_table(self, table_name):
        return "TRUNCATE TABLE {0}".format(table_name)

    def insert(self, table_name, values, source):
        statement = "INSERT INTO {0} ".format(table_name)
        statement = statement + "\n(\n {0} \n)".format(','.join(values))
        statement = statement + " " + str(source)
        return statement


    def update(self, table_name, values, data, source='', where=[]):
        statement = "UPDATE {0}\nSET ".format(table_name)
        params = map(lambda data: "{0} = {1}"
        .format(data[0],data[1]), zip(values,data))
        statement += ",".join(params)
        if source:
            statement += "\nFROM\n" + str(source)
        if where:
            statement += " WHERE " + " and ".join(map(lambda a: '(' + a + ')',where)) + "\n"
        return statement


    def delete(self, table_name, where=[]):
        statement = "DELETE FROM {0} ".format(table_name)
        if where:
            statement = statement + "WHERE {0}".format(",".join(where))
        return statement


    def select(self, values=['*'], sources=[], join_types=[], join_conditions=[],
                where=[], group=[], order=[], order_asc='ASC'):
        statement = " SELECT {0}\n".format(",".join(values))
        if sources:
            statement += " FROM {0}\n".format(sources[0])
            if len(sources) > 1:
                joins = map(lambda t: "{0} {1} on {2} \n"
                .format(t[1], t[0], " and ".join(t[2])), zip(sources[1:],join_types,join_conditions))
                statement += " ".join(joins)
            if where:
                statement += " WHERE " + " and ".join(map(lambda a: '(' + a + ')',where)) + "\n"
            if group:
                statement += " GROUP BY " + ",".join(group) + "\n"
            if order:
                statement += " ORDER BY " + ",".join(order) + ' ' + order_asc + "\n"
        return statement

    def select_into(self, table_name, values=['*'], sources=[],join_types=[]
                    ,join_conditions=[], where=[], group=[], order=[], order_asc='ASC'):
        values + ' INTO ' + table_name
        return self.select(values, sources, join_types, join_conditions, where, group, order, order_asc )  

    def create_procedure(self, name, code, params=[]):
        statement = "CREATE PROCEDURE {0} ".format(name)
        if params: statement += ', '.join(map(lambda p: '@' + p ))
        statement += "\nAS\n SET NOCOUNT ON;\n"
        statement += code + '\n'
        statement += "GO;"
        return statement


    def alter_procedure(self, name, code, params=[]):
        statement = "ALTER PROCEDURE {0} ".format(name)
        if params: statement += ', '.join(map(lambda p: '@' + p ))
        statement += "\nAS\n SET NOCOUNT ON;\n"
        statement += code + '\n'
        statement += "GO;"
        return statement

    def get_columns(self,database_name,table_name):
        table = "{0}.INFORMATION_SCHEMA.COLUMNS".format(database_name)
        conditions = ["TABLE_NAME = N'{0}'".format(table_name)]
        return self.select(["COLUMN_NAME"],[table],[],[],conditions,[])

    def get_key(self,database_name,table_name):
        tables = ["{0}.INFORMATION_SCHEMA.TABLE_CONSTRAINTS TC".format(database_name),
                "{0}.INFORMATION_SCHEMA.KEY_COLUMN_USAGE KU".format(database_name)]
        join_types = ["INNER JOIN"]
        join_conditions = [["TC.CONSTRAINT_TYPE = 'PRIMARY KEY'",
                        "TC.CONSTRAINT_NAME = KU.CONSTRAINT_NAME",
                        "KU.table_name='{0}'".format(table_name)]]
        orders = ["KU.TABLE_NAME", "KU.ORDINAL_POSITION"]
        return self.select(['COLUMN_NAME'],tables,join_types,join_conditions,[],orders)

    def create_temporary_table(self, table_name, column_names, column_types, column_nullable):
        columns = map(lambda c: "{0} {1} {2}".format(c[0], c[1], "NOT NULL" if not c[2] else "NULL"),
                                zip(column_names, column_types, column_nullable))
        statement = "DECLARE @{0} table ({1});".format(table_name, ", ".join(columns))
        return (statement, "@"+table_name)

    def declare_variable(self, name, data_type, value=None):
        if value:
            return "DECLARE {0} {1} = {3};".format(name, data_type, value)
        return "DECLARE @{0} {1};".format(name, data_type)

    def set_variable(self, name, value):
        return "SET @{0} = {1};".format(name, value) 

    def equals(self, a, b):
        return "{0} = {1}".format(str(a), str(b))

    def different(self, a, b):
        return "{0} <> {1}".format(str(a), str(b))

    def is_grater(self, a, b):
        return "{0} > {1}".format(str(a), str(b))

    def is_grater_or_equal(self, a, b):
        return "{0} >= {1}".format(str(a), str(b))

    def is_less(self, a, b):
        return "{0} < {1}".format(str(a), str(b))
    
    def is_less_or_equal(self, a, b):
        return "{0} <= {1}".format(str(a), str(b))

    def between(self, element, left_limit, right_limit):
        return "{0} BETWEEN {1} AND {2}".format(str(element), str(left_limit), str(right_limit))

    def is_null(self, element, null):
        if null:
            return "{0} IS NULL".format(str(element))
        return "{0} IS NOT NULL".format(str(element))

    def in_(self, element, source, is_in):
        if is_in:
            return "{0} IN {1}".format(str(element), str(source))
        return "NOT {0} IN {1}".format(str(element), str(source))

    def order_asc(self,asc):
        return 'ASC' if asc else 'DESC'

    def union(self, sources):
        return '\nUNION\n'.join(sources)

    def count(self, element, distinct):
        if distinct:
            return "COUNT(DISTINCT {0})".format(element)
        return "COUNT({0})".format(str(element))

    def sum(self, element):
        return "SUM({0})".format(str(element))

    def max(self, element):
        return "MAX({0})".format(str(element))

    def min(self, element):
        return "MIN({0})".format(str(element))