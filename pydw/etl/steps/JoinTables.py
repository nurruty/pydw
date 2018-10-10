from pydw.etl.steps.Step import Step
from pydw.dw import Query
from pydw.dw import Table
from copy import deepcopy

class JoinTables(Step):

    def code(self):
        return self._execute()[0]

    def output(self):
        output = dict()
        output['table'] = self._execute()[1]
        return output

    def _execute(self):
        table1 = self.input[0]
        table2 = self.input[1]

        if self.data.get('columns_table1'):
            column_names_1 = self.data['columns_table1']
            columns = [table1.columns.get(c) for c in column_names_1]
        else:
            columns = table1.get_column_list()

        if self.data.get('columns_table2'):
            column_names_2 = self.data['columns_table2']
            columns +=  [table2.columns.get(d) for d in column_names_2]
        else:
            columns +=  table2.get_column_list()


        if self.data.get('join'):
            join_types = [self.data['join']]

        if self.data.get('key_table1'):
            key_table1 = self.data['key_table1']

        if self.data.get('key_table2'):
            key_table2 = self.data['key_table2']

        join_conditions = [ ]

        for cols in zip(key_table1, key_table2):

            cond = table1.columns[cols[0]].equals(table2.columns[cols[1]])
            join_conditions.append(cond)


        temp_name =  self.name + '_'

        aux_table = Table(self.dbms, 'aux', deepcopy(columns))
        (code,temp_table) = aux_table.create_temporary(temp_name)


        query = Query(
                    dbms=self.dbms,
                    sources= [table1, table2],
                    columns= columns,
                    join_types= join_types,
                    join_conditions= [join_conditions]
                )


        code += temp_table.insert(
            query = query
        )

        print(temp_table.name)
        return(code, temp_table)
