from pydw.etl.steps.Step import Step
from pydw.dw import Query, Column

class InitDimension(Step):

    def code(self):
        return self._execute()[0]

    def output(self):
        output = dict()
        return output

    def _execute(self):
        dimension = self.input[0]

        statements = dimension.truncate()

        code = dimension.insert_empty_row()

        cond = Query(
            dbms= self.dbms,
            sources=[dimension],
            columns=[Column(self.dbms.count(dimension.surrogate_key.name), dimension.surrogate_key.data_type)]
        )

        statements+= self.dbms.if_begin_end(cond.code(), code)

        return (statements,'')