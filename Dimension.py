from Table import Table
from DMS import DMS_TYPE

class Dimension(Table):

  def __init__(self,dms_type,name,columns,key = [],query = [], virtual = False):
    Table.__init__(self,dms_type,name,columns,key = key, query = query, virtual= virtual)

  def update(self,values,data,source = [],conditions = []):
    raise "Type 1 SCDimensions cannot be updated"



class SCDimension1(Table):
  def __init__(self,dms_type,name,columns,key = [],query = [], virtual = False):
    Table.__init__(self,dms_type,name,columns,key = key, query = query, virtual= virtual)

  def insert(self,values,source,join_conditions):
    #key_list = map(lambda (key1,key2): "{0} = {1}".format(key1,key2), zip(join_key, self.key))
    return self.select_join(sources = source + [self.name],
    join_types = ["LEFT JOIN"], join_conditions = join_conditions, conditions= ["{0} is null".format(self.key[0])])


  



class SCDimension2(Table):
  def __init__(self,dms_type,name,columns,valid_column,init_column, end_column
  ,key = [],query = [], virtual = False):
    columns + [valid_column,init_column,end_column]
    Table.__init__(self,dms_type,name,columns,key = key, query = query, virtual= virtual)

    self.valid_column = valid_column
    self.init_column = init_column
    self.end_column = end_column


  @classmethod
  def fromTable(cls,dms_type,cursor,database_name,table_name,
    valid_column,init_column, end_column, conditions = []):
    table = Table.fromTable(dms_type,cursor,database_name,table_name,
    conditions)
    return cls(dms_type,table.name,table.columns,valid_column,init_column, end_column
    ,key = table.key, query = table.query)


  @classmethod
  def fromQuery(cls,dms_type,cursor,table_name,valid_column,init_column, end_column,
        values = ["*"],sources= [],join_types = [],join_conditions = [], conditions = [] ,order = [] ):

        table = Table.fromQuery(dms_type,cursor,table_name,values,sources,join_types,
        join_conditions,conditions,order)
        return cls(dms_type,table_name,values,valid_column,init_column,
        end_column, query = table.query, virtual=True)

  
  
