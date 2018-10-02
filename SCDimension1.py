from Dimension import Dimension
from DBMS import DBMS_TYPE
from Query import Query

class SCDimension1(Dimension):

    def __init__(self, dbms, name, columns, sk_column, nk_columns, alias=''):
        Dimension.__init__(self, dbms, name, columns, sk_column,
                           nk_columns, alias)

    def update_scd1(self, source, join_key=[]):

        join_conditions = ["{0} = {1}".format(c[0],c[1])
                            for c in zip(join_key,self.natural_key)]
        query = Query(
                    dbms = self.dbms,
                    sources = [source] + [self],
                    columns = source.get_column_list(),
                    join_types = ["LEFT JOIN"],
                    join_conditions = join_conditions,
                    where= [self.surrogate_key.null()],
                )

        values = self.get_column_names()
        return self.dbms.insert(self.name, values, query.code())