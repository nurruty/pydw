from Step import Step
from pydw.dw import SCDimension1

class SCD1DimensionsLoad(Step):

  def code(self):
      return ''

  def output(self):
      output = dict()
      output['dimension'] = self._execute()[1]
      return output

  def _execute(self):

      cursor = self.input[0]
      if self.data.get('database'):
        database_name = self.data['database']

      if self.data.get('schema'):
        schema_name = self.data['schema']

      if self.data.get('dimension_name'):
        dimension_name = [self.data['dimension_name']]

      dimension = SCDimension1.from_db(
          dbms = self.dbms,
          cursor = cursor,
          database_name = database_name,
          schema_name = schema_name,
          table_name = dimension_name,
          alias = self.name
      )

      return('', dimension)
