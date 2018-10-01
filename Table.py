from SQLServer import SQLServer
from DBMS import DBMS_TYPE
from Column import Column
from copy import deepcopy

def __dbms__(dbms_type):
    if dbms_type == DBMS_TYPE.SQL_SERVER:
        return SQLServer()

class Table(object):
    
    def __init__(self, dbms_type, name, columns, key=[], query=[], alias=''):
        self.name = name
        self.columns = dict()
        for c in columns:
            c.container_name = name if not alias else alias
            self.columns[c.name] = c
        self.key = key if len(key) > 0 else []
        self.query = "(" + query +")" + name if len(query) > 0 else ""
        self.alias = alias
        self.dbms_type = dbms_type
        self.dbms = __dbms__(dbms_type)


    @classmethod
    def from_db(cls,dbms_type, cursor, database_name, schema_name, table_name, where=[], alias=''):
        dbms = __dbms__(dbms_type)
        query = dbms.get_columns(database_name,table_name)
        cursor.execute(query)
        columns = [column[0] for column in cursor.fetchall()]
        query = dbms.get_key(database_name,table_name)
        cursor.execute(query)
        key = cursor.fetchall()
        key = map(lambda k: k[0], key) if key else []
        table_name = database_name + "." + schema_name + "." + table_name
        query = dbms.select(columns, [table_name] ,where = where)
        return cls(dbms_type,table_name,map(lambda c: Column(c[0],c[1],c[2],c[3]), columns),key,query,alias)


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

    def insert(self, columns, query):
        values = [c.name for k,c in columns.items()]
        return self.dbms.insert(self.name, values, query.code())

    def update(self, columns, data, where=[]):
        column_names = [c.name for k,c in columns.items()]
        return self.dbms.update(self.name, column_names, data, where=where)

    def update_from(self, columns, source, where=[]):
        column_names = [c.name for k,c in columns.items()]
        data = map(lambda c: c.container_name + '.' + c.name + ' ' + c.alias, source.columns)
        if isinstance(source, Table):
            source_code = source.name + ' ' + source.alias
        else:
            source_code = '(' + source.code() + ') ' + source.alias
        return self.dbms.update(self.name, column_names, data, source_code, where)
    
    def delete(self, where=[]):
        return self.dbms.delete(self.name, where=where)

    def get_column_list(self):
        copy = deepcopy(self.columns)
        list_ = []
        for k,c in copy.items():
            #c.container_name = self.alias
            list_.append(c)
        return list_

    

    