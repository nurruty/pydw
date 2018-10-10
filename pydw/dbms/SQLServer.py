from pydw.dbms.DBMS import DBMS

class SQLServer(DBMS):

    def create_table(self, table_name, column_names, column_types, column_nullable, key=[], foreign_keys=[]):
        statement = " CREATE TABLE {0} \n(\n".format(table_name)
        statement += ",".join(map(lambda c: "{0} {1} {2}".format(c[0], c[1], " NOT NULL\n" if not c[2] else " NULL \n"),
                                zip(column_names, column_types, column_nullable)))
        if key:
            statement += " CONSTRAINT [PK_{0}] PRIMARY KEY CLUSTERED ( {1} ASC ) \n".format(table_name,",".join(key))
        statement +=  " ) \n"
        return statement

    def drop_table(self, table_name):
        return " DROP TABLE {0}; \n".format(table_name)

    def add_column(self, table_name, column, data_type):
        statement = " ALTER TABLE {0} \n".format(table_name)
        statement += " ADD {0} {1}; \n".format(column,data_type)
        return statement

    def drop_column(self, table_name, column_name):
        statement = " ALTER TABLE {0} \n".format(table_name)
        statement += " DROP COLUMN {0}; \n".format(column_name)
        return statement

    def alter_column(self, table_name, column_name, data_type):
        statement = " ALTER TABLE {0} \n".format(table_name)
        statement += " ALTER COLUMN {0} {1}; \n".format(column_name,data_type)
        return statement

    def truncate_table(self, table_name):
        return " TRUNCATE TABLE {0} \n".format(table_name)

    def insert(self, table_name, values, source):
        statement = " INSERT INTO {0} \n".format(table_name)
        statement = statement + "( {0} )".format(',\n'.join(values))
        statement = statement + " " + str(source)
        return statement


    def update(self, table_name, values, data, source='', where=[]):
        statement = "\n UPDATE {0} \nSET ".format(table_name)
        params = ["{0} = {1}\n".format(d[0],d[1]) for d in zip(values,data)]
        statement += ",".join(params)
        if source:
            statement += " FROM\n" + str(source)
        if where:
            statement += " WHERE " + " and ".join(['(' + w + ')' for w in where]) + ""
            statement += '\n'
        return statement


    def delete(self, table_name, where=[]):
        statement = " DELETE FROM {0} \n".format(table_name)
        if where:
            statement = statement + " WHERE {0} ".format(",".join(where))
        return statement


    def select(self, values=['*'], sources=[], join_types=[], join_conditions=[],
                where=[], group=[], order=[], order_asc='ASC'):
        statement = " SELECT {0} \n".format(",".join(values))
        if sources:
            statement += " FROM {0} \n".format(sources[0])
            if len(sources) > 1:
                joins = list(map(lambda t: "{0} {1} on {2} "
                .format(t[1], t[0], " and ".join(t[2])), zip(sources[1:],join_types,join_conditions)))
                statement += " ".join(joins)
            if where:
                statement += " WHERE " + " and ".join(list(map(lambda a: '(' + a + ')',where))) + ""
                statement += '\n'
            if group:
                statement += " GROUP BY " + ",".join(group) + "\n"
            if order:
                statement += " ORDER BY " + ",".join(order) + ' ' + order_asc + "\n"
        return statement

    def select_into(self, table_name, values=['*'], sources=[], join_types=[]
                    ,join_conditions=[], where=[], group=[], order=[], order_asc='ASC'):
        values + ' INTO ' + table_name + '\n'
        return self.select(values, sources, join_types, join_conditions, where, group, order, order_asc )  

    def create_procedure(self, database_name, schema_name, name, code, params=[]):
        statement = " USE [{0}]\n".format(database_name)
        statement += " SET ANSI_NULLS ON\n GO\n SET QUOTED_IDENTIFIER ON\n GO\n"
        statement += " CREATE PROCEDURE {0}.{1} AS\n".format(schema_name, name)
        if params: statement += ', '.join(map(lambda p: '@' + p ))
        statement += " BEGIN \n SET NOCOUNT ON; \n"
        statement += code + 'END'
        return statement


    def alter_procedure(self, database_name, schema_name, name, code, params=[]):
        statement = " USE [{0}] \n".format(database_name)
        statement += " SET ANSI_NULLS ON\n GO\n SET QUOTED_IDENTIFIER ON\n GO\n"
        statement += " ALTER PROCEDURE {0}.{1} AS".format(schema_name, name)
        if params: statement += ', '.join(map(lambda p: '@' + p ))
        statement += "BEGIN \n SET NOCOUNT ON; \n"
        statement += code + 'END'
        return statement

    def get_columns(self,database_name,table_name):
        values = [" COLUMN_NAME, DATA_TYPE + " 
        + "case when DATA_TYPE = 'numeric' or DATA_TYPE = 'decimal'"
        + "then '(' + convert(varchar(3),NUMERIC_PRECISION) +','+ convert(varchar(3),NUMERIC_SCALE) + ')'"
	    + "when DATA_TYPE = 'varchar' or DATA_TYPE = 'char'"
        + "then '(' + convert(varchar(5),CHARACTER_MAXIMUM_LENGTH) + ')'"
	    + "else '' end	 DATA_TYPE",
        " case when IS_NULLABLE = 'YES' then 1 else 0 end NULLABLE",
        " COLUMNPROPERTY(object_id(TABLE_SCHEMA+'.'+TABLE_NAME), COLUMN_NAME, 'IsIdentity') IS_IDENTITY"
        ]
        table = " {0}.INFORMATION_SCHEMA.COLUMNS".format(database_name)
        conditions = [" TABLE_NAME = N'{0}'".format(table_name)]
        return self.select(values=values,sources=[table],where=conditions)

    def get_key(self,database_name,table_name):
        tables = [" {0}.INFORMATION_SCHEMA.TABLE_CONSTRAINTS TC".format(database_name),
                " {0}.INFORMATION_SCHEMA.KEY_COLUMN_USAGE KU".format(database_name)]
        join_types = [" INNER JOIN"]
        join_conditions = [[" TC.CONSTRAINT_TYPE = 'PRIMARY KEY'",
                        " TC.CONSTRAINT_NAME = KU.CONSTRAINT_NAME",
                        " KU.table_name='{0}'".format(table_name)]]
        orders = [" KU.TABLE_NAME", " KU.ORDINAL_POSITION"]
        return self.select(['COLUMN_NAME'],sources=tables,join_types=join_types
                ,join_conditions=join_conditions,order=orders)

    # def create_temporary_table(self, table_name, column_names, column_types, column_nullable):
    #     columns = map(lambda c: "{0} {1} {2}".format(c[0], c[1], "NOT NULL" if not c[2] else "NULL"),
    #                             zip(column_names, column_types, column_nullable))
    #     statement = " DECLARE @{0} table ({1});".format(table_name, ", ".join(columns))
    #     return (statement, "@"+table_name)

    def create_temporary_table(self, table_name, column_names, column_types):
        columns = map(lambda c: "{0} {1}".format(c[0], c[1]),
                                zip(column_names, column_types))
        statement = " DECLARE @{0} table ({1}); \n".format(table_name, ", ".join(columns))
        return (statement, "@"+table_name)

    def declare_variable(self, name, data_type, value=None):
        if value:
            return " DECLARE {0} {1} = {3}; \n".format(name, data_type, value)
        return " DECLARE @{0} {1}; \n".format(name, data_type)

    def set_variable(self, name, value):
        return " SET @{0} = {1};".format(name, value) 

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
            return "{0} IN ({1})".format(str(element), str(source))
        return " NOT {0} IN ({1})".format(str(element), str(source))

    def order_asc(self,asc):
        return 'ASC' if asc else 'DESC'

    def union(self, sources):
        return 'UNION'.join(sources)

    def count(self, element='', distinct=False):
        element = '*' if not element else element
        if distinct:
            return " COUNT(DISTINCT {0})".format(element)
        return " COUNT({0})".format(str(element))

    def sum(self, element):
        return " SUM({0})".format(str(element))

    def max(self, element):
        return " MAX({0})".format(str(element))

    def min(self, element):
        return " MIN({0})".format(str(element))

    def today(self):
        return " GETDATE()"

    def empty_date(self):
        return "'1753-01-01 00:00:00.000'"

    def type_number(self, n, r=0):
        return " numeric({0},{1})".format(n,r)

    def is_numeric(self, data_type):
        return data_type.find('numeric') != -1

    def type_int(self):
        return " int"

    def is_int(self, data_type):
        return data_type.find('int') != -1

    def type_varchar(self, l):
        return " varchar({0})".format(l)

    def is_char(self, data_type):
        return data_type.find('char') != -1

    def type_date(self):
        return " date"

    def is_date(self, data_type):
        return data_type.find('date') != -1

    def null_value(self):
        return " NULL"