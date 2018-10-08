from pydw.dw.Dimension import Dimension
from pydw.dw.Query import Query

class SCDimension1(Dimension):

    def __init__(self, dbms, name, columns, sk_column, nk_columns, alias=''):
        Dimension.__init__(self, dbms, name, columns, sk_column,
                           nk_columns, alias)


    def update_scd1(self, source, join_key=[], where=[]):

        join_conditions = ["{0} = {1}".format(c[0].get_full_name(),c[1].get_full_name())
                            for c in zip(join_key,self.natural_key)]


        query = Query(
                    dbms = self.dbms,
                    sources = [source] + [self],
                    columns = source.get_column_list(),
                    join_types = ["LEFT JOIN"],
                    join_conditions = [join_conditions],
                    where= [self.surrogate_key.null()] + where,
                )

        columns_aux = self.columns_not_in([self.surrogate_key.name])
        return self.insert(query, columns_aux)