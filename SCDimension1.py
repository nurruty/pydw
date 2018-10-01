from Table import Table
from DBMS import DBMS_TYPE
from Query import Query

class SCDimension1(Table):

    def __init__(self,dms_type,name,columns,key = [],query = [], virtual = False):
        Table.__init__(self,dms_type,name,columns,key = key, query = query)

    def update_scd1(self, dbms, source, join_conditions=[]):
        
        query = Query(
                    sources = [source] + [self],
                    columns = source.columns, 
                    join_types = ["LEFT JOIN"], 
                    join_conditions = join_conditions,
                    where= [dbms.is_null(self.key[0], True)],
                )

       
        values = [c.name for k, c in self.columns.items()]
        return self.dbms.insert(self.name, values, query.code(dbms))