from pydw.etl.steps.Step import Step
from pydw.dw import Query

class SCDimension2Update(Step):

    def code(self):
        return self._execute()[0]

    def output(self):
        output = dict()
        output['dimension'] = self._execute()[1]
        return output

    def _execute(self):
        table = self.input[0]
        scd_table = self.input[1]


        if self.data.get('natural_key'):
            scd_columns = scd_table.get_column_list()
            natural_key = [c for c in scd_columns if c.name in self.data['natural_key']]
        else:
            natural_key = scd_table.natural_key

        if self.data.get('conditions'):
            where = self.data['conditions']
        else:
            where = []

        if self.data.get('audited_columns'):
            scd_columns = scd_table.get_column_list()
            audited_columns = [c for c in scd_columns if c.name in self.data['audited_columns']]
        else:
            audited_columns = scd_table.get_column_list()


        code = scd_table.update_scd2(
            source = table,
            join_key = natural_key,
            where= where,
            audited_columns = audited_columns
        )

        return(code, scd_table)
