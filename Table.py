from Sql_server import Sql_server
from DMS import DMS_TYPE

class Table(object):
  def __init__(self,dms_type,name,columns,key = [],query = [], virtual = False):
    self.name = name
    self.columns = columns
    self.key = key if len(key) > 0 else []
    self.query = "(" + query +")" + name if len(query) > 0 else ""
    self.dms_type = dms_type
    self.dms = _init_dms_(dms_type)
    self.virtual = virtual


  @classmethod
  def fromTable(cls,dms_type,cursor,database_name,table_name,where = []):
    dms = _init_dms_(dms_type)
    query = dms.get_columns(database_name,table_name)
    cursor.execute(query)
    columns = map(lambda column: column[0], cursor.fetchall())
    query = dms.get_key(database_name,table_name)
    cursor.execute(query)
    key = cursor.fetchall()
    key = map(lambda k: k[0], key) if len(key) > 0 else []
    query = dms.select(columns, [database_name + ".dbo." + table_name],where = where)
    return cls(dms_type,table_name,columns,key,query)


  @classmethod
  def fromQuery(cls,dms_type,table_name,values = ["*"],sources= [],
        join_types = [],join_conditions = [], where = [] ,order = [] ):
        dms = _init_dms_(dms_type)
        query = dms.select(values,sources,join_types,
        join_conditions,where,order)
        return cls(dms_type,table_name,values, query = query, virtual=True)

  @classmethod
  def fromTextQuery(cls,dms_type,table_name,query):
        columns = query[len("SELECT"):].split(",")
        return cls(dms_type,' ' + table_name + ' ',columns, query = query, virtual=True)

  def truncate(self):
    self.dms.truncate(self.name)

  def insert(self,values,source):
    return self.dms.insert(self.name,values,source.query)

  def update(self,values,data,source,where = []):
    #tables = map(lambda s: s.query if len(s.query) > 0 else s.name, source)
    return self.dms.update(self.name,values,data,
      source.query, where= where)

  def delete(self,target,where = []):
    return self.dms.delete(self.name,where= where)

  def select(self,values = ['*'],source = [],join_types = []
    ,join_conditions = [], where = [], order = [], group = []):

    tables = map(lambda s: s.query if len(s.query) > 0 else s.name, source)
    return self.dms.select(values, [self.name] + tables, join_types= join_types,
      join_conditions= join_conditions,where = where, order= order, group = [])

  def aggregate(self,operations,operation_values,other_values = [], where = [], order = [], group = []):
    aggregations = map(lambda v: "{0}({1})".format(v[0], v[1]), zip(operations,operation_values))
    return self.dms.select(
      values = aggregations + other_values,
      sources= [self.name],
      where= where,
      group= group,
      order= order
    )

""" class Column:
  def __init__(self,name,col_type,is_null,is_autonumber):
        self.name = name
        self.col_type = col_type
        self.is_null = True if not is_null else False
        self.is_autonumber = True if is_autonumber else False """


#helpers
def _init_dms_(dms_type):
  if dms_type == DMS_TYPE.SQL_SERVER:
    return Sql_server(dms_type)
