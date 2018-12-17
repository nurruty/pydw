from pydw.etl.steps.Step import Step
from pydw.dw import Query, Column, Table
from copy import deepcopy


class AggregateValues(Step):
# This step spects a table as input, columns as data,
# and returns the code to create an stagin table, and the
# resulting Table

    def code(self):
        return self._execute()[0]

    def output(self):
        output = dict()
        output['table'] = self._execute()[1]
        return output

    def _execute(self):
        table = self.input[0]

        operations = []
        aggregate_columns = []
        column_data = []
        data_types = dict()
        where = []

        if self.data.get('operations'):
            operations = self.data['operations']
        if self.data.get('aggregate_columns'):
            aggregate_columns = self.data['aggregate_columns']
        if self.data.get('columns'):
            column_data = self.data['columns']
        if self.data.get('metadata'):
            data_types = self.data['metadata']
        if self.data.get('where'):
            where = self.data['where']

        columns = dict()
        for c in column_data:
            if isinstance(c, tuple):
                columns[c[0]] = Column(c[0], '', data= c[1], alias=c[0])
            else:
                col = table.columns[c]
                columns[c] = deepcopy(col)

        for a in zip(aggregate_columns, operations):
          op = a[1]
          c = table.columns[a[0]]
          coltxt = op + '({0})'.format(c.name)
          col = Column(name = op + '_' + c.name, data_type = c.data_type, data= coltxt, alias= op + '_' + c.alias)
          columns[op + '_' + c.alias] = col

        column_list = [ c for k,c in columns.items() ]

        for c in column_list:
            if data_types.get(c.name):
                c.data_type = data_types[c.name]

        temp = Table(self.dbms, '', column_list)

        temp_name = self.name + '_' + table.alias
        (code,temp_table) = temp.create_temporary(temp_name)

        query = Query(
            dbms = self.dbms,
            sources = [table],
            columns= column_list,
            where = where,
            group= column_data,
            alias = 'temp'
        )


        code += temp_table.insert(
            query = query
        )

        for k,c in temp_table.columns.items():
            c.data = c.name

        self.data['where'] = []

        return(code, temp_table)


