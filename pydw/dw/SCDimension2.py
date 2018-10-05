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

    

    def update_scd2(self, source, join_key=[], where=[], audited_columns=[]):

        join_conditions = [["{0} = {1}".format(c[0].get_full_name(),c[1].get_full_name())
                            for c in zip(join_key,self.natural_key)]]

        columns_aux = self.columns_not_in([self.surrogate_key.name])
        columns_aux_names = [c.name for c in columns_aux]
        nk_aux_names = [c.name for c in self.natural_key]

        if not audited_columns:
            audited_columns = columns_aux

        (statements, new_table) = self.create_temporary('new', 
                                    columns_aux_names, key_column_names= nk_aux_names)

        (aux_code, changed_table) = self.create_temporary('changed', 
                                    columns_aux_names, key_column_names= nk_aux_names)
        statements += aux_code


        # #When source is a query, the data is saved in a temporal table
        # if not source.alias:
        #     aux = 'a{0}'.format(str(aux_alias))
        #     source.alias = aux
        #     aux_alias += 1

        #query_colums = [c for c in self.get_column_list() if c.name in source.get_column_names() ]

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


        statements += self.dbms.insert(
            table_name = new_table.name,
            values = [c.name for c in columns_aux],
            source = query.code()
        )



        audited_column_names = [c.name for c in audited_columns]
        query = Query(
            dbms = self.dbms,
            sources = [self,source],
            columns = list(map(lambda v: v[1] if v[0].name in audited_column_names else v[0],
                      zip(columns_aux,source.get_column_list()))),
            join_types = ["JOIN"],
            join_conditions= join_conditions,
            where = [t.different(s) for (t,s) in zip(columns_aux,source.get_column_list())
                     if t.name in audited_column_names]
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

        statements += self.dbms.insert(
            table_name = changed_table.name,
            values = [c.name for c in columns_aux],
            source = query.code()
        )

        query = Query(
            dbms = self.dbms,
            columns = [changed_table.key[0]],
            sources = [changed_table]
        )

        statements += self.update(
            columns = [self.valid_column, self.end_column],
            data = ['0',self.dbms.today()],
            where = [self.natural_key[0].in_(query,False),
                     self.valid_column.equals(1,False)
                    ]
        )

        new_rows = Query(self.dbms, sources = [new_table],
                         columns=new_table.get_column_list())

        new_rows.union(Query(self.dbms, sources = [changed_table],
                         columns=changed_table.get_column_list()))

        statements += self.insert(
            query = new_rows,
            columns= columns_aux,
        )

        return statements


