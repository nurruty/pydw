from pydw.etl.steps.Step import Step
from copy import deepcopy

class UpdateFromTable(Step):

  def code(self):
      return self._execute()[0]

  def output(self):
      output = dict()
      output['table'] = self._execute()[1]
      return output

  def _execute(self):
      table1 = self.input[0]
      table2 = self.input[1]


      target_column_names = []
      source_column_names = []

      target_columns = []
      source_columns = []

      target_key = []
      source_key = []

      if self.data.get('columns_table1'):
        target_column_names = self.data['columns_table1']

      target_columns = [c for c in table1.get_column_list() if c.name in target_column_names]

      if self.data.get('columns_table2'):
        source_column_names = self.data['columns_table2']

      source_columns = [c for c in table2.get_column_list() if c.name in source_column_names]


      if self.data.get('key_table1'):
        for k in self.data['key_table1']:
          target_key.append(table1.columns[k])

      if self.data.get('key_table2'):
        for k in self.data['key_table2']:
          source_key.append(table2.columns[k])

      where = [c[0].equals(c[1], False) for c in zip(target_key,source_key)]

      if self.data.get('other_conditions'):
        where += self.data['other_conditions']


      code = table1.update_from_table(
          columns = target_columns,
          sources = [table2],
          source_columns = source_columns,
          where = where,
      )

      return(code, table1)
