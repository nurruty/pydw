from pydw.dw.Table import Table
from enum import Enum

class DIMENSION_TYPE(Enum):
    SCDimension1 = 1
    SCDimension2 = 2
    
class Dimension(Table):

    def __init__(self, dbms, name, columns, sk_column, nk_columns=[], alias=''):
        Table.__init__(self, dbms,name, columns, key=[sk_column], alias=alias)


        self.surrogate_key = sk_column
        self.natural_key = nk_columns
        


    @classmethod
    def from_db(cls, dbms, cursor, database_name, schema_name, table_name,
                nk_column_names=[], where = [], alias=''):

        table = Table.from_db(dbms, cursor, database_name, schema_name,
                    table_name, where, alias)
        if nk_column_names:
            nk = [c for k,c in table.columns.items() if c.name in nk_column_names]
        else:
            nk = table.key

        sk = table.key[0]

        return cls(dbms, table.name, table.get_column_list(), sk, nk, table.alias)


    def get_not_nullable_columns(self):
        return [c for k,c in self.columns.items() 
                if not c.is_null and c.name != self.surrogate_key.name]