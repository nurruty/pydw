from SQLServer import SQLServer
from DBMS import DBMS_TYPE
from Column import Column
from copy import deepcopy

def __dbms__(dbms_type):
    if dbms_type == DBMS_TYPE.SQL_SERVER:
        return SQLServer()

class Table(object):

    def __init__(self, dbms, name, columns, key=[], alias=''):
        self.name = name
        self.columns = dict()
        for c in columns:
            c.container_name = name if not alias else alias
            c.dbms = dbms
            self.columns[c.name] = c
        self.key = key if len(key) > 0 else []
        self.alias = alias
        self.dbms = dbms


    @classmethod
    def from_db(cls,dbms, cursor, database_name, schema_name, table_name, where=[], alias=''):
        query = dbms.get_columns(database_name,table_name)
        cursor.execute(query)
        columns = [Column(c[0],data_type=c[1],is_null=int(c[2]),is_autonumber=int(c[3]))
                   for c in cursor.fetchall()]
        query = dbms.get_key(database_name,table_name)
        cursor.execute(query)
        key = cursor.fetchall()
        key = [k[0] for k in key]
        table_name = database_name + "." + schema_name + "." + table_name
        column_names = [c.name for c in columns]
        query = dbms.select(column_names, [table_name] ,where = where)
        return cls(dbms, table_name, columns, key=key, alias=alias)


    def create(self):
        column_names = []
        column_types = []
        column_nullable = []
        column_autonumber = []
        column_foreign_key = []
        for c in self.columns:
            column_names.append(c.name)
            column_types.append(c.data_type)
            column_nullable.append(c.is_null)
            column_autonumber.append(c.is_autonumber)
            column_foreign_key.append(c.foreign_key_table_name, c.foreign_key_column)
        return self.dbms.create_table(self.name,column_names, column_types, 
                                column_autonumber, self.key, column_foreign_key)

    def drop(self):
        return self.dbms.drop_table(self.name)

    def truncate(self):
        return self.dbms.truncate_table(self.name)

    def insert(self, query, columns=[]):
        if not columns:
            columns = self.columns
        values = [c.name for k,c in self.columns.items()]
        return self.dbms.insert(self.name, values, query.code())

    def update(self, columns=[], data=[], where=[]):
        if not columns:
            columns = self.columns
        column_names = [c.name for k,c in self.columns.items()]
        return self.dbms.update(self.name, column_names, data, where=where)

    def update_from(self, columns=[], source=None, source_columns=[], where=[]):
        if not columns:
            columns = self.get_column_list()
            print(columns)
        column_names = [c.name for c in columns]

        if not source_columns:
            source_columns = source.get_column_list()
        data = [c.get_full_name() for c in source_columns]

        if isinstance(source, Table):
            source_code = source.name + ' ' + source.alias
        else:
            source_code = '(' + source.code() + ') ' + source.alias
        return self.dbms.update(table_name= self.name, values=column_names,
                                data=data, source=source_code, where=where)

    def delete(self, where=[]):
        return self.dbms.delete(self.name, where=where)

    def get_column_list(self):
        copy = deepcopy(self.columns)
        list_ = []
        for k,c in copy.items():
            #c.container_name = self.alias
            list_.append(c)
        return list_

    def get_column_names(self):
        return [c.name for k,c in self.columns.items()]

    def get_column_types(self):
        return [c.data_type for k,c in self.columns.items()]

    def get_column_nullables(self):
        return [c.is_null for k,c in self.columns.items()]

    def set_columns_container(self, container_name):
        for c in self.columns:
            c.container_name = container_name

