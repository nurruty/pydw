from pydw.etl.steps.Step import Step
from pydw.dw import Query, Column, Table
from copy import deepcopy


class SelectValues(Step):
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
        column_data = []
        data_types = dict()

        if self.data.get('columns'):
            column_data = self.data['columns']
        if self.data.get('metadata'):
            data_types = self.data['metadata']

        columns = dict()
        for c in column_data:
            if isinstance(c, tuple):
                columns[c[0]] = Column(c[0], '', data= c[1], alias=c[0])
            else:
                col = table.columns[c]
                columns[c] = deepcopy(col)

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
            alias = 'temp'
        )


        code += temp_table.insert(
            query = query
        )

        for k,c in temp_table.columns.items():
            c.data = c.name

        return(code, temp_table)


