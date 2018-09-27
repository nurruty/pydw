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
  def fromTable(cls,dms_type,cursor,database_name,table_name,where = []):
    dms = _init_dms_(dms_type)
    query = dms.get_columns(database_name,table_name)
    cursor.execute(query)
    columns = map(lambda column: column[0], cursor.fetchall())
    query = dms.get_key(database_name,table_name)
    cursor.execute(query)
    key = cursor.fetchall()
    key = map(lambda k: k[0], key) if len(key) > 0 else []
    query = dms.select(columns, [table_name],where = where)
    return cls(dms_type,table_name,columns,key,query)


  @classmethod
  def fromQuery(cls,dms_type,cursor,table_name,values = ["*"],sources= [],
        join_types = [],join_conditions = [], where = [] ,order = [] ):
        dms = _init_dms_(dms_type)
        query = dms.select(values,sources,join_types,
        join_conditions,where,order)
        return cls(dms_type,table_name,values, query = query, virtual=True)


  def insert(self,values,source):
    return self.dms.insert(self.name,values,source)

  def update(self,values,data,source = [],where = []):
    return self.dms.update(self.name,values,data,
      source = source, where= where)

  def delete(self,target,where = []):
    return self.dms.delete(self.name,where= where)

  def select(self,values = ['*'] ,where = [], order = []):
    return self.dms.select(values, [self.name],
      where = where, order= order)

  def select_join(self,values = ['*'],sources = [],join_types = []
    ,join_conditions = [], where = [], order = []):

    return self.dms.select(values, [self.name] + sources, join_types= join_types,
      join_conditions= join_conditions,where = where, order= order)


class Column:
  def __init__(self,name,col_type,is_null,is_autonumber):
        self.name = name
        self.col_type = col_type
        self.is_null = True if not is_null else False
        self.is_autonumber = True if is_autonumber else False


#helpers
def _init_dms_(dms_type):
  if dms_type == DMS_TYPE.SQL_SERVER:
    return Sql_server(dms_type)
