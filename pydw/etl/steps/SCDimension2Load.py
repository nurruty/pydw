from Step import Step
from pydw.dw import SCDimension2

class SCDimension2Load(Step):

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
        dimension_name = self.data['dimension_name']

      if self.data.get('valid_column'):
        valid_column = self.data['valid_column']

      if self.data.get('init_column'):
        init_column = self.data['init_column']

      if self.data.get('end_column'):
        end_column = self.data['end_column']

      if self.data.get('nk_column_names'):
        nk_names = self.data['nk_column_names']

      dimension = SCDimension2.from_db(
          dbms = self.dbms,
          cursor = cursor,
          database_name = database_name,
          schema_name = schema_name,
          table_name = dimension_name,
          valid_column_name = valid_column,
          init_column_name = init_column,
          end_column_name = end_column,
          nk_column_names = nk_names,
          alias = self.name
      )

      return('', dimension)
