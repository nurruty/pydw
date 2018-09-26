from DMS import DMS

class Sql_server(DMS):

  def insert(self,target,values,source):
    statement = "INSERT INTO {0}".format(target)
    statement = statement + "({0})".format(','.join(values))
    statement = statement + " " + str(source)
    return statement


  def update(self,target,values,data,source = [],conditions = []):
    statement = "UPDATE {0} ".format(target)
    if len(source) == 0:
      params = map(lambda (column,value): "{0} = {1}"
      .format(column,value), zip(values,data))
      statement = statement + ",".join(params)
    return statement


  def delete(self,target,conditions = []):
    statement = "DELETE FROM {0} ".format(target)
    statement = statement + "WHERE {0}".format(",".join(conditions))
    return statement


  def select(self,value,sources,join_types = []
    ,join_conditions = [], conditions = [], order = []):
      statement = "SELECT {0} FROM {1} ".format(",".join(value),sources[0])
      if len(sources) > 1:
        joins = map(lambda (source,join_type, condition_list): "{0} {1} on {2}"
        .format(join_type, source, " and ".join(condition_list)), zip(sources[1:],join_types,join_conditions))
        statement = statement + " ".join(joins)
      if len(conditions) > 0:
        statement = statement + " WHERE " + " and ".join(conditions)
      if len(order):
        statement = statement + " ORDER BY " + ",".join(order)
      return statement


  def get_columns(self,database_name,table_name):
    table = "{0}.INFORMATION_SCHEMA.COLUMNS".format(database_name)
    conditions = ["TABLE_NAME = N'{0}'".format(table_name)]
    return self.select(["COLUMN_NAME"],[table],[],[],conditions,[])


  def get_key(self,database_name,table_name):
    tables = ["{0}.INFORMATION_SCHEMA.TABLE_CONSTRAINTS TC".format(database_name),
              "{0}.INFORMATION_SCHEMA.KEY_COLUMN_USAGE KU".format(database_name)]
    join_types = ["INNER JOIN"]
    join_conditions = [["TC.CONSTRAINT_TYPE = 'PRIMARY KEY'",
                      "TC.CONSTRAINT_NAME = KU.CONSTRAINT_NAME",
                      "KU.table_name='{0}'".format(table_name)]]
    orders = ["KU.TABLE_NAME", "KU.ORDINAL_POSITION"]
    return self.select(['COLUMN_NAME'],tables,join_types,join_conditions,[],orders)

