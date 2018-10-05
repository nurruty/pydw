from pydw.etl.steps.Step import Step

class SCDimension1Update(Step):

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
            scd_columns = table.get_column_list()
            natural_key = [c for c in scd_columns if c.name in self.data['natural_key']]
        else:
            natural_key = table.key

        if self.data.get('conditions'):
            where = self.data['conditions']
        else:
            where = []

        code = scd_table.update_scd1(
            source = table,
            join_key = natural_key,
            where= where
        )

        return(code, scd_table)
