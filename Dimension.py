from Table import Table
from DBMS import DBMS_TYPE

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