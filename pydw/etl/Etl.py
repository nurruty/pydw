
from dwobjects import Table, Query, SCDimension2

class Etl:

    def __init__(self, cursor, dbms, database_name, schema_name, name, params=[], code = ''):
        self.database_name = database_name
        self.schema_name = schema_name
        self.cursor = cursor
        self.dbms = dbms
        self.name = name
        self.params = params
        self.code = code


    def create(self):
        code = self.dbms.create_procedure(self.database_name,
                        self.schema_name, self.name, self.code, self.params)
        return code

    def alter(self):
        code = self.dbms.alter_procedure(self.database_name,
                        self.schema_name, self.name, self.code, self.params)
        return code

    def drop(self):
        code = self.dbms.drop_procedure(self.name)
        self.cursor.execute(code)

    def execute(self):
        self.cursor.execute(self.code)

    def add(self,code):
        self.code += code

    def send_email(self):
        pass