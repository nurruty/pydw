
class Etl:

    def __init__(self,cursor,dms,name,code = ''):
        self.cursor = cursor
        self.dms = dms
        self.name = name
        self.code = code


    def create(self):
        code = self.dms.create_procedure(self.name) + self.code + self.dms.end_procedure(self.name)
        self.cursor.execute(code)

    def alter(self):
        code = self.dms.alter_procedure(self.name) + self.code + self.dms.end_procedure(self.name)
        self.cursor.execute(code)

    def drop(self):
        code = self.dms.drop_procedure(self.name)
        self.cursor.execute(code)

    def execute(self):
        self.cursor.execute(self.code)

    def add(self,code):
        self.code += code

    def send_email(self):
        pass