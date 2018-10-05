from pydw.etl.steps.Step import Step
from pydw.dw import Query

class PrepareDimension(Step):

  def code(self):
      return self._execute()[0]

  def output(self):
      output = dict()
      output['table'] = self._execute()[1]
      return output

  def _execute(self):

    table = self.input[0]
    target_table = self.input[1]

    surrogate_key = target_table.surrogate_key


    if self.data.get('incoming_columns'):
        columns = target_table.get_column_list()
        incoming_columns = [c for c in columns if c.name in self.data['incoming_columns']]
    else:
        incoming_columns = target_table.get_column_list()


    (code,today_table) = target_table.create_temporary(
                                table_name = 'hoy',
                                not_column_names= [surrogate_key.name]
                            )

    query = Query(
        dbms = self.dbms,
        columns= table.get_column_list(),
        sources= [table]
    )

    code += today_table.insert(
        query = query,
        columns = incoming_columns
    )


    if len(incoming_columns) < (len(target_table.get_column_list()) - 1):
        not_nullable_columns = target_table.get_not_nullable_columns()
        not_nullable_columns = [c for c in not_nullable_columns
                                if c.name not in self.data.get('incoming_columns')]
        not_nullable_data = [c.empty_value() for c in not_nullable_columns]

        code += today_table.update(
            columns = not_nullable_columns,
            data = not_nullable_data
        )

    return (code, today_table)