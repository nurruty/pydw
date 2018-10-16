from pydw.etl.steps.Step import Step
from pydw.dw import Query, Table
from copy import deepcopy

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
    natural_key_names = [c.name for c in target_table.natural_key]


    if self.data.get('incoming_columns'):
        columns = target_table.get_column_list()
        incoming_columns = [c for c in columns if c.name in self.data['incoming_columns']]
    else:
        incoming_columns = target_table.get_column_list()

    if self.data.get('temporal-table') == True:
        (code,today_table) = target_table.create_temporary(
                                    table_name = 'today',
                                    not_column_names= [surrogate_key.name]
                                )
    else:
        today_table = Table.from_table(self.dbms, target_table, 'today', not_column_names=[surrogate_key.name], key_column_names = natural_key_names)

        code = today_table.create()


    query = Query(
        dbms = self.dbms,
        columns= table.get_column_list(),
        sources= [table]
    )

    if len(incoming_columns) < (len(target_table.get_column_list()) - 1):
        not_nullable_columns = target_table.get_not_nullable_columns()
        not_nullable_columns = [deepcopy(c) for c in not_nullable_columns
                                if c.name not in self.data.get('incoming_columns')]

        for c in not_nullable_columns:
            c.data = c.empty_value()
            c.container_name = ''

        incoming_columns += not_nullable_columns
        query.add_columns(not_nullable_columns)

    code += today_table.insert(
        query = query,
        columns = incoming_columns
    )


    return (code, today_table)