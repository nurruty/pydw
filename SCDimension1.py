from Table import Table
from DMS import DMS_TYPE

class SCDimension1(Table):

  def __init__(self,dms_type,name,columns,key = [],query = [], virtual = False):
    Table.__init__(self,dms_type,name,columns,key = key, query = query, virtual= virtual)

  def update_scd1(self,values=[],source = [],source_values = ['*'],join_conditions = []):
    if len(values) == 0:
      values = self.columns

    tables = map(lambda s: s.query if len(s.query) > 0 else s.name, source)
    temporary = Table.fromQuery(self.dms_type,'', values = source_values, sources = tables + [self.name],
                join_types = ["LEFT JOIN"], join_conditions = join_conditions,
                where= ["{0} is null".format(self.key[0])])

    return self.dms.insert(self.name,values,temporary.query)