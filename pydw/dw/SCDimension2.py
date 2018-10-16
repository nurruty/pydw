from pydw.dw.Dimension import Dimension
from pydw.dw.Column import Column
from pydw.dw.Query import Query
from copy import deepcopy


class SCDimension2(Dimension):

    def __init__(self, dbms, name, columns ,valid_column ,init_column, end_column,
                 sk_column, nk_columns, alias = ''):

        Dimension.__init__(self, dbms, name, columns, sk_column,
                           nk_columns, alias)

        self.valid_column = valid_column
        self.init_column = init_column
        self.end_column = end_column

    @classmethod
    def from_db(cls, dbms, cursor, database_name, schema_name, table_name,
                valid_column_name, init_column_name, end_column_name, nk_column_names=[],
                where=[], alias=''):

        dimension = Dimension.from_db(dbms, cursor, database_name, schema_name,
                 table_name, nk_column_names, where, alias)

        valid_column = dimension.columns[valid_column_name]
        init_column = dimension.columns[init_column_name]
        end_column = dimension.columns[end_column_name]

        return cls(dbms, dimension.name, dimension.get_column_list(), valid_column, init_column, end_column
        ,dimension.surrogate_key, dimension.natural_key, dimension.alias)

    def insert_empty_row(self):

        columns = [c for k,c in self.columns.items() if c.name != self.surrogate_key.name]
        new_columns = []

        for c in columns:
            new_col = deepcopy(c)
            if c.name == self.valid_column.name:
                new_col.data = '1'
            elif c.name == self.end_column.name:
                new_col.data = self.dbms.null_value()
            else:
                new_col.data = c.empty_value()
            new_col.container_name = ''
            new_columns.append(new_col)

        temp = Query(dbms= self.dbms, columns=new_columns)

        return self.insert(
             query= temp,
             columns = columns
        )

    def update_scd2(self, source, join_key=[], where=[], audited_columns=[]):

        join_conditions = [["{0} = {1}".format(c[0].get_full_name(),c[1].get_full_name())
                            for c in zip(join_key,self.natural_key)]]

        columns = self.columns_not_in([self.surrogate_key.name])
        columns_names = [c.name for c in columns]
        nk_names = [c.name for c in self.natural_key]

        if not audited_columns:
            audited_columns = columns


        (statements, new_table) = self.create_temporary('new', 
                                    columns_names, key_column_names= nk_names)

        (aux_code, changed_table) = self.create_temporary('changed', 
                                    columns_names, key_column_names= nk_names)
        statements += aux_code

        # Insert new rows into temporary table
        statements += self._get_new_rows_table(columns, source,
                             join_conditions, where, new_table)


        # Insert changed rows into temporary table
        statements += self._get_changed_rows_table(columns, source,
                             join_conditions, audited_columns, changed_table)

        # Invalidate changed rows
        statements += self._invalidate_changed_rows(changed_table)

        # Insert new and changed rows in scdimension2
        statements += self._insert_rows(columns, new_table, changed_table)

        # Update static data
        statements += self._update_static_columns(source, join_key, audited_columns)

        return statements


    def _get_changed_rows_table(self, columns, source, join_conditions, audited_columns, temporary_table):

        audited_column_names = [c.name for c in audited_columns]
        conditions = [t.different(s) for (t,s) in zip(columns,source.get_column_list())
                     if t.name in audited_column_names]
        or_conditions = " OR ".join(conditions)

        query = Query(
            dbms = self.dbms,
            sources = [self,source],
            columns = list(map(lambda v: v[1] if v[0].name in audited_column_names else v[0],
                      zip(columns,source.get_column_list()))),
            join_types = ["JOIN"],
            join_conditions= join_conditions,
            where = [or_conditions]
        )


        query.update_columns(
                    columns =[query.columns[self.valid_column.name],
                        query.columns[self.init_column.name],
                        query.columns[self.end_column.name]
                    ],
                    new_columns = [Column("1", self.dbms.type_number(1),False),
                          Column(self.dbms.today(), self.dbms.type_date(),False),
                          Column(self.dbms.null_value(), self.dbms.type_date())
                    ]
                )

        return  self.dbms.insert(
            table_name = temporary_table.name,
            values = [c.name for c in columns],
            source = query.code()
        )

    def _get_new_rows_table(self, columns, source, join_conditions, where, temporary_table):

        query = Query(
            dbms = self.dbms,
            sources = [source,self],
            columns= source.get_column_list(),
            join_types = ["LEFT JOIN"],
            join_conditions = join_conditions,
            where = [self.surrogate_key.null()] + where,
            alias = 'new'
        )


        query.update_columns(
            columns =[query.columns[self.valid_column.name],
                query.columns[self.init_column.name],
                query.columns[self.end_column.name]
            ],
            new_columns = [Column("1", self.dbms.type_number(1),False),
                    Column(self.dbms.today(), self.dbms.type_date(),False),
                    Column(self.dbms.null_value(), self.dbms.type_date())
            ]
        )


        return self.dbms.insert(
            table_name = temporary_table.name,
            values = [c.name for c in columns],
            source = query.code()
        )



    def _invalidate_changed_rows(self, changed_rows_table):

        query = Query(
            dbms = self.dbms,
            columns = [changed_rows_table.key[0]],
            sources = [changed_rows_table]
        )

        return self.update(
            columns = [self.valid_column, self.end_column],
            data = ['0',self.dbms.today()],
            where = [self.natural_key[0].in_(query,False),
                     self.valid_column.equals(1,False)
                    ]
        )


    def _insert_rows(self, columns, new_rows_table, changed_rows_table):

        new_rows = Query(self.dbms, sources = [new_rows_table],
                         columns=new_rows_table.get_column_list())

        new_rows.union(Query(self.dbms, sources = [changed_rows_table],
                         columns=changed_rows_table.get_column_list()))

        return self.insert(
            query = new_rows,
            columns= columns,
        )

    def _update_static_columns(self, source, join_key, audited_columns):

        audited_column_names = [c.name for c in audited_columns]

        static_columns = self.columns_not_in(audited_column_names)
        static_columns = [c for c in static_columns
                                if c.name != self.surrogate_key.name
                                and c.name != self.valid_column.name
                                and c.name != self.init_column.name
                                and c.name != self.end_column.name]

        static_columns_names = [c.name for c in static_columns]

        source_columns = [c for c in source.get_column_list()
                        if c.name in static_columns_names]

        join_conditions = [["{0} = {1}".format(c[0].get_full_name(),c[1].get_full_name())
                                for c in zip(join_key,self.natural_key)]]

        natural_key_names = [c.name for c in self.natural_key]

        query = Query(
            dbms = self.dbms,
            sources = [self,source],
            columns = source_columns,
            join_types = ["JOIN"],
            join_conditions= join_conditions
        )

        source_key = [c for c in query.get_column_list()
                        if c.name in natural_key_names ]

        return self.update_from_query(
            columns= static_columns,
            source= query,
            source_columns= source_columns,
            target_key= self.natural_key,
            source_key= source_key,
            where = [self.valid_column.equals(1, False)]
        )