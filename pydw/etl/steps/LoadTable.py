from Step import Step
from dwobjects import Table

class LoadTable(Step):

  def code(self):
      return ''

  def output(self):
      table = self._execute()[1]
      output = dict()
      output['table'] = table
      return output

  def _execute(self):

      cursor = self.input[0]
      if self.data.get('database'):
        database_name = self.data['database']

      if self.data.get('schema'):
        schema_name = self.data['schema']

      if self.data.get('table_name'):
        table_name = self.data['table_name']

      table = Table.from_db(
          dbms = self.dbms,
          cursor = cursor,
          database_name = database_name,
          schema_name = schema_name,
          table_name = table_name,
          alias = self.name
      )



      return('', table)
