from pydw.dw.Table import Table
from copy import deepcopy


class Query:
    
    def __init__(self, dbms, sources = [] , columns=[], join_types=[], join_conditions=[], where=[], group=[], order=[], order_asc=True, alias=''):
        self.sources = sources #can be tables or other queries
        self.columns = dict()
        for c in columns:
            c.dbms = dbms
            self.columns[c.name] = c
        self.join_types = join_types
        self.join_conditions = join_conditions
        self.where = where
        self.group = group
        self.order = order
        self.order_asc = order_asc
        self.alias = alias
        self.union_queries = []
        self.dbms = dbms

    def add_columns(self, columns):
        for c in columns:
            self.columns[c.name] = c

    def get_column_list(self):
        copy = deepcopy(self.columns)
        list_ = []
        for k,c in copy.items():
            c.container_name = self.alias
            list_.append(c)
        return list_

    def add_where_conditions(self, where):
        self.where.append(where)

    def add_join(self, tables, join_type, join_conditions):
        self.sources.append(tables)
        self.join_types.append(join_type)
        self.join_conditions.append(join_conditions)

    def add_group_columns(self, group):
        self.group.append(group)

    def add_order_columns(self, order):
        self.order.append(order)

    def change_order_asc(self, asc):
        self.order_asc = asc

    def union(self, query):
        self.union_queries.append(query)

    def code(self):
        column_names = []
        sources_code = []
        for k,c in self.columns.items():
            if c.container_name:
                column_names.append('.'.join([c.container_name,c.data]) + ' ' + c.alias)
            else:
                column_names.append(c.data + ' ' + c.alias)

        if self.sources:
            sources_code = [s.name + ' ' + s.alias if isinstance(s, Table) 
                            else '(' + s.code() + ') ' + s.alias for s in self.sources]



        query_code = self.dbms.select(
                        values = column_names,
                        sources = sources_code,
                        join_types = self.join_types,
                        join_conditions = self.join_conditions,
                        where = self.where,
                        group = self.group,
                        order = self.order,
                        order_asc = self.dbms.order_asc(self.order_asc)
                    )

        if self.union_queries:
            union_code = [u.code() for u in self.union_queries]
            return self.dbms.union([query_code] + union_code)
        return query_code


    def into_table(self, table_name):

        column_names = []
        sources_code = []
        for k,c in self.columns.items():
            if c.container_name:
                column_names.append('.'.join([c.container_name,c.data]) + ' ' + c.alias)
            else:
                column_names.append(c.data + ' ' + c.alias)

        if self.sources:
            sources_code = [s.name + ' ' + s.alias if isinstance(s, Table)
                            else '(' + s.code() + ') ' + s.alias for s in self.sources]

        code = self.dbms.select_into(
                    table_name = table_name,
                    values = column_names,
                    sources = sources_code,
                    join_types = self.join_types,
                    join_conditions = self.join_conditions,
                    where = self.where,
                    group = self.group,
                    order = self.order,
                    order_asc = self.dbms.order_asc(self.order_asc)
                )

        table = Table(self.dbms, table_name, self.get_column_list())

        return (code, table)

    def get_column_names(self):
        return [c.name for k,c in self.columns.items()]

    def get_column_types(self):
        return [c.data_type for k,c in self.columns.items()]

    def get_column_nullables(self):
        return [c.is_null for k,c in self.columns.items()]

    def set_columns_container(self, container_name):
        for k,c in self.columns.items():
            c.container_name = container_name

    def update_columns(self, columns, new_columns):
        for cols in zip(columns,new_columns):
            self.columns[cols[0].name] = cols[1]