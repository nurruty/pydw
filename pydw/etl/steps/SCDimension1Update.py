from Step import Step

class SCDimension1Update(Step):

    def code(self):
        return self._execute()[0]

    def output(self):
        output = dict()
        output['table'] = self._execute()[1]
        return output

    def _execute(self):
        table = self.input[0]
        scd_table = self.input[1]
        natural_key = self.data['natural_key']

        code = scd_table.update_scd2(
            source = table,
            join_key = [natural_key]
        )

        return(code, scd_table)
