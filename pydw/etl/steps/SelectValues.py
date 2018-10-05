from Step import Step
from pydw.dw import Query


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

        if self.data.get('columns'):
            column_names = self.data['columns']
        if self.data.get('metadata'):
            data_types = self.data['metadata']

        columns = [table.columns[c] for c in column_names]

        for c in columns:
            if data_types.get(c.name):
                c.data_type = data_types[c.name]

        temp_name =  self.name + '_' + table.alias
        (code,temp_table) = table.create_temporary(temp_name, column_names)

        query = Query(
            dbms = self.dbms,
            sources = [table],
            columns= temp_table.get_column_list(),
            alias = 'temp'
        )


        code += temp_table.insert(
            query = query
        )

        return(code, temp_table)


