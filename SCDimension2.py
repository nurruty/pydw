from Table import Table
from Column import Column
from Query import Query
from DBMS import DBMS_TYPE
from copy import deepcopy


class SCDimension2(Table):

    def __init__(self, dbms, name, columns ,valid_column ,init_column, end_column,
                 key = [],alias = ''):

        #columns += [valid_column,init_column,end_column]
        Table.__init__(self, dbms, name, columns, key=key, alias=alias)

        self.valid_column = valid_column
        self.init_column = init_column
        self.end_column = end_column

    @classmethod
    def from_db(cls, dbms, cursor, database_name, schema_name, table_name,
        valid_column_name, init_column_name, end_column_name, where = [], alias=''):

        table = Table.from_db(dbms, cursor, database_name, schema_name,
                 table_name, where, alias)

        #columns = table.columns + [valid_column, init_column, end_column]
        valid_column = table.columns[valid_column_name]
        init_column = table.columns[init_column_name]
        end_column = table.columns[end_column_name]

        return cls(dbms, table.name, table.get_column_list(), valid_column, init_column, end_column
        ,key = table.key, alias = table.alias)

    def update_scd2(self, source, join_conditions=[], where=[], audited_columns=[]):

        aux_alias = 1

        if not audited_columns:
            audited_columns = self.columns

        (statements,new_var) = self.dbms.create_temporary_table(
            table_name = "new",
            column_names = self.get_column_names(),
            column_types = self.get_column_types(),
            column_nullable = self.get_column_nullables()
        )

        new_table = deepcopy(self)
        new_table.name = new_var

        (aux, changed_var) = self.dbms.create_temporary_table(
            table_name = "changed",
            column_names = self.get_column_names(),
            column_types = self.get_column_types(),
            column_nullable = self.get_column_nullables()
        )

        changed_table = deepcopy(self)
        changed_table.name = changed_var

        statements += aux

        #When source is a query, the data is saved in a temporal table
        if not source.alias:
            aux = 'a{0}'.format(str(aux_alias))
            source.alias = aux
            aux_alias += 1

        query = Query(
            dbms = self.dbms,
            sources = [source,self],
            columns= source.get_column_list(),
            join_types = ["LEFT JOIN"],
            join_conditions = join_conditions,
            where = [self.columns[self.key[0]].null()] + where,
            alias = 'new'
        )

        query.add_columns([Column("1", self.dbms.type_number(1),False),
                          Column(self.dbms.today(), self.dbms.type_date(),False),
                          Column(self.dbms.null_value(), self.dbms.type_date())
                          ])


        statements += self.dbms.insert(
            table_name = new_var,
            values = self.get_column_names(),
            source = query.code()
        )

        audited_column_names = [c.name for c in audited_columns]
        query = Query(
            dbms = self.dbms,
            sources = [self,source],
            columns = list(map(lambda v: v[1] if v[0].name in audited_column_names else v[0]
                    , zip(self.get_column_list(),source.get_column_list()))),
            join_types = ["JOIN"],
            join_conditions= join_conditions,
            where = [t.different(s) for (t,s) in zip(self.get_column_list(),source.get_column_list())
                     if t.name in audited_column_names]
        )

        query.add_columns([Column("1", self.dbms.type_number(1),False),
                          Column(self.dbms.today(), self.dbms.type_date(),False),
                          Column(self.dbms.null_value(), self.dbms.type_date())
                          ])

        statements += self.dbms.insert(
            table_name = changed_var,
            values = self.get_column_names(),
            source = query.code()
        )

        query = Query(
            dbms = self.dbms,
            columns = [changed_table.columns[changed_table.key[0]]],
            sources = [changed_table]
        )

        statements += self.update(
            columns = [self.valid_column, self.end_column],
            data = ['0',self.dbms.today()],
            where = [self.columns[self.key[0]].in_(query),
                    self.valid_column.equals(1)
                    ]
        )

        new_rows = Query(self.dbms, sources = [new_table],
                         columns=new_table.get_column_list())

        new_rows.union(Query(self.dbms, sources = [changed_table],
                         columns=changed_table.get_column_list()))

        statements += self.insert(
            query = new_rows,
            columns= self.get_column_list(),
        )

        return statements