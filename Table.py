from Sql_server import Sql_server
from DMS import DMS_TYPE
from Column import Column

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
    return cls(dms_type,table_name,map(lambda c: Column(c[0],c[1],c[2],c[3])),key,query)


  @classmethod
  def fromQuery(cls,dms_type,table_name,values = ["*"],tables= [],
        join_types = [],join_conditions = [], where = [] ,order = [] ):
        dms = _init_dms_(dms_type)
        query = dms.select(values,tables,join_types,
        join_conditions,where,order)
        return cls(dms_type,table_name,values, query = query, virtual=True)

  @classmethod
  def fromTextQuery(cls,dms_type,table_name,query):
        columns = query[len("SELECT"):].split(",")
        return cls(dms_type,' ' + table_name + ' ',columns, query = query, virtual=True)

  def create(self):
    columns = map(lambda c: "{0} {1} {2}".format(c.name,c.col_type,c.is_null))
    self.dms.create_table(self.name,columns,self.key)

  def drop(self):
    self.dms.drop(self.name)

  def truncate(self):
    self.dms.truncate(self.name)

  def insert(self,values,table):
    return self.dms.insert(self.name,values,table.query)

  def update(self,column_names,data,where = []):
    return self.dms.update(self.name,column_names,data,
      '',where)

  def update_from(self,values,table,where = []):
    columns = map(lambda c: "{0}_".format(c.name), table.columns)
    return self.dms.update(self.name,values,columns,table.query,where)

  def delete(self,target,where = []):
    return self.dms.delete(self.name,where= where)

  def select(self,values = ['*'], tables = [],join_types = []
            ,join_conditions = [], where = [], order = [], group = []):
    
    sources = map(lambda t: t.query if len(t.query) > 0 else t.name, tables)
    return self.dms.select(values, [self.name] + sources, join_types= join_types,
      join_conditions= join_conditions,where = where, order= order, group = [])

  def select_into(self,table_name,values = ['*'], tables = [],join_types = []
                  ,join_conditions = [], where = [], order = [], group = []):
    
    sources = map(lambda t: t.query if len(t.query) > 0 else t.name, tables)
    return self.select_into(table_name, values, sources,join_types
    ,join_conditions, where, order, group )

  def aggregate(self,operations,operation_values,other_values = [], where = [], order = [], group = []):
    aggregations = map(lambda v: "{0}({1})".format(v[0], v[1]), zip(operations,operation_values))
    return self.dms.select(
      values = aggregations + other_values,
      sources= [self.name],
      where= where,
      group= group,
      order= order
    )


#helpers
def _init_dms_(dms_type):
  if dms_type == DMS_TYPE.SQL_SERVER:
    return Sql_server(dms_type)