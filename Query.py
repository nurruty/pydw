
from DBMS import DBMS_TYPE
from SQLServer import SQLServer
from Table import Table
from copy import deepcopy


class Query:
    
    def __init__(self,sources = [] , columns=[], join_types=[], join_conditions=[], where=[], group=[], order=[], order_asc=True, alias=''):
        self.sources = sources #can be tables or other queries
        self.columns = deepcopy(columns)
        self.join_types = join_types
        self.join_conditions = join_conditions
        self.where = where
        self.group = group
        self.order = order
        self.order_asc = order_asc
        self.alias = alias
        self.union_queries = []

    def add_columns(self, columns):
        self.columns.append(columns)

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
    
    def code(self,dbms):
        column_names = [c.container_name + '.' + c.name + ' ' + c.alias for c in self.columns]
        if self.sources:
            sources_code = [s.name + ' ' + s.alias if isinstance(s, Table) 
                            else '(' + s.code(dbms) + ') ' + s.alias for s in self.sources]
        
        query_code = dbms.select(
                        values = column_names,
                        sources = sources_code,
                        join_types = self.join_types,
                        join_conditions = self.join_conditions,
                        where = self.where,
                        group = self.group,
                        order = self.order,
                        order_asc = dbms.order_asc(self.order_asc)
                    )

        if self.union_queries:
            return dbms.union([query_code] + self.union_queries)
        return query_code


 