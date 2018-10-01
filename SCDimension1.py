from Table import Table
from DBMS import DBMS_TYPE
from Query import Query

class SCDimension1(Table):

    def __init__(self, dbms, name, columns,key=[], alias=''):
        Table.__init__(self, dbms, name, columns, key=key, alias='')

    def update_scd1(self, source, join_conditions=[]):

        query = Query(
                    dbms = self.dbms,
                    sources = [source] + [self],
                    columns = source.get_column_list(), 
                    join_types = ["LEFT JOIN"], 
                    join_conditions = join_conditions,
                    where= [self.dbms.is_null(self.key[0], True)],
                )

        values = [c.name for k, c in self.columns.items()]
        return self.dbms.insert(self.name, values, query.code())