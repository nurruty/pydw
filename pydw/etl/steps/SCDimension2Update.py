from pydw.etl.steps.Step import Step
from pydw.dw import Query

class SCDimension2Update(Step):

    def code(self):
        return self._execute()[0]

    def output(self):
        output = dict()
        output['table'] = self._execute()[1]
        return output

    def _execute(self):
        table = self.input[0]
        scd_table = self.input[1]

        surrogate_key = scd_table.surrogate_key

        if self.data.get('natural_key'):
            scd_columns = scd_table.get_column_list()
            natural_key = [c for c in scd_columns if c.name in self.data['natural_key']]
        else:
            natural_key = scd_table.natural_key

        if self.data.get('incoming_columns'):
            scd_columns = scd_table.get_column_list()
            incoming_columns = [c for c in scd_columns if c.name in self.data['incoming_columns']]
        else:
            incoming_columns = scd_table.get_column_list()

        if self.data.get('conditions'):
            where = self.data['conditions']
        else:
            where = []

        if self.data.get('audited_columns'):
            scd_columns = scd_table.get_column_list()
            audited_columns = [c for c in scd_columns if c.name in self.data['audited_columns']]
        else:
            audited_columns = scd_table.get_column_list()


        (code,today_table) = scd_table.create_temporary(
                                table_name = 'hoy',
                                not_column_names= [surrogate_key.name]
                            )

        query = Query(
            dbms = self.dbms,
            columns= table.get_column_list(),
            sources= [table]
        )

        code += today_table.insert(
            query = query,
            columns = incoming_columns
        )

        if len(incoming_columns) <= len(scd_table.get_column_list()) - 1:
            not_nullable_columns = scd_table.get_not_nullable_columns()
            not_nullable_columns = [c for c in not_nullable_columns 
                                    if c.name not in self.data.get('incoming_columns')]
            not_nullable_data = [c.empty_value() for c in not_nullable_columns]

            code += today_table.update(
                columns = not_nullable_columns,
                data = not_nullable_data
            )


        code+= scd_table.update_scd2(
            source = today_table,
            join_key = natural_key,
            where= where,
            audited_columns = audited_columns
        )

        return(code, scd_table)
