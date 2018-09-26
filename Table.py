from Sql_server import Sql_server
from DMS import DMS_TYPE

class Table(object):
  def __init__(self,dms_type,name,columns,key = [],query = [], virtual = False):
    self.name = name
    self.columns = columns
    self.key = key if len(key) > 0 else []
    self.query = query if len(query) > 0 else ""
    self.dms = _init_dms_(dms_type)
    self.virtual = virtual


  @classmethod
  def fromTable(cls,dms_type,cursor,database_name,table_name,conditions = []):
    dms = _init_dms_(dms_type)
    query = dms.get_columns(database_name,table_name)
    cursor.execute(query)
    columns = map(lambda column: column[0], cursor.fetchall())
    query = dms.get_key(database_name,table_name)
    cursor.execute(query)
    key = cursor.fetchall()
    key = map(lambda k: k[0], key) if len(key) > 0 else columns[0]
    query = dms.select(columns, [table_name],conditions = conditions)
    return cls(dms_type,table_name,columns,key,query)


  @classmethod
  def fromQuery(cls,dms_type,cursor,table_name,values = ["*"],sources= [],
        join_types = [],join_conditions = [], conditions = [] ,order = [] ):
        dms = _init_dms_(dms_type)
        query = dms.select(values,sources,join_types,
        join_conditions,conditions,order)
        return cls(dms_type,table_name,values, query = query, virtual=True)


  def insert(self,values,source):
    return self.dms.insert(self.name,values,source)

  def update(self,values,data,source = [],conditions = []):
    return self.dms.update(self.name,values,data,
      source = source, conditions= conditions)

  def delete(self,target,conditions = []):
    return self.dms.delete(self.name,conditions= conditions)

  def select(self,values = ['*'] ,conditions = [], order = []):
    return self.dms.select(values, [self.name],
      conditions = conditions, order= order)

  def select_join(self,values = ['*'],sources = [],join_types = []
    ,join_conditions = [], conditions = [], order = []):

    return self.dms.select(values, [self.name] + sources, join_types= join_types,
      join_conditions= join_conditions,conditions = conditions, order= order)

#helpers
def _init_dms_(dms_type):
  if dms_type == DMS_TYPE.SQL_SERVER:
    return Sql_server(dms_type)
