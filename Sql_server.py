from DMS import DMS 

class Sql_server(DMS):
      
  #def create_table(self,name,columns,key):
        

  def insert(self,target,values,source):
    statement = "INSERT INTO {0} \n".format(target)
    statement = statement + "\n(\n {0} \n)".format(','.join(values))
    statement = statement + " " + str(source)
    return statement


  def update(self,target,values,data,source = [], where = []):
    statement = "UPDATE {0}\nSET ".format(target)
    params = map(lambda column,value: "{0} = {1}"
    .format(column,value), zip(values,data))
    statement += ",".join(params)
    if len(source) > 0:
      statement += "FROM\n" + str(source)
    if len(where) > 0:
        statement += "WHERE " + " and ".join(map(lambda a: '(' + a + ')',where)) + "\n"
    return statement


  def delete(self,target,where = []):
    statement = "DELETE FROM {0} ".format(target)
    statement = statement + "WHERE {0}".format(",".join(where))
    return statement


  def select(self,value,sources,join_types = []
    ,join_conditions = [], where = [], order = [],group = []):

      statement = "SELECT {0}\n".format(",".join(value))
      statement += "FROM {0}\n".format(sources[0])

      if len(sources) > 1:
        joins = map(lambda t: "{0} {1} on {2} \n"
        .format(t[1], t[0], " and ".join(t[2])), zip(sources[1:],join_types,join_conditions))
        statement += " ".join(joins)
      if len(where) > 0:
        statement += "WHERE " + " and ".join(map(lambda a: '(' + a + ')',where)) + "\n"
      if len(order) > 0:
        statement += "ORDER BY " + ",".join(order) + "\n"
      if len(group) > 0:
        statement += "GROUP BY " + ",".join(group) + "\n"   
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