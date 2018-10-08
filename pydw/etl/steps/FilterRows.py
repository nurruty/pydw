from pydw.etl.steps.Step import Step
from pydw.dw import Query

class FilterRows(Step):

    def code(self):
        return self._execute()[0]

    def output(self):
        output = dict()
        output['table'] = self._execute()[1]
        return output

    def _execute(self):
        table = self.input[0]
        where = self.data['conditions']

        temp_name =  self.name + '_' + table.name
        (code,temp_table) = table.create_temporal(temp_name)

        query = Query(self.dbms, self.input[0], where=where)

        code += self.dbms.insert(
            table_name = temp_table.name,
            source = query.code()
        )

        return(code, temp_table)
