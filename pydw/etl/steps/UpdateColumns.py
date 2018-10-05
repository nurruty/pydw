from Step import Step

class UpdateColumns(Step):

  def code(self):
      return self._execute()[0]

  def output(self):
      output = dict()
      output['table'] = self._execute()[1]
      return output

  def _execute(self):
      table = self.input[0]
      columns = self.data['columns']
      values = self.data['values']
      where = self.data['conditions']

      code = table.update(
          columns = columns,
          data = values,
          where = where,
      )

      return(code, table)
