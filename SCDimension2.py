from Table import Table
from DMS import DMS_TYPE

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
    valid_column,init_column, end_column, where = []):
    table = Table.fromTable(dms_type,cursor,database_name,table_name,
    where)
    return cls(dms_type,table.name,table.columns,valid_column,init_column, end_column
    ,key = table.key, query = table.query)


  @classmethod
  def fromQuery(cls,dms_type,table_name,valid_column,init_column, end_column,
                values = ["*"],sources= [],join_types = [],
                join_conditions = [], where = [] ,order = []):

        table = Table.fromQuery(dms_type,table_name,values,sources,join_types,
        join_conditions,where,order)
        return cls(dms_type,table_name,values,valid_column,init_column,
        end_column, query = table.query, virtual=True)


  def update_scd2(self,values=[],source = [],
                  source_values = ['*'],join_conditions = [], where_conditions=[]):

        tables = map(lambda s: s.query if len(s.query) > 0 else s.name, source)

        if len(values) == 0:
          values = self.columns

        new = self.select(values = source_values, source = tables,
                join_types = ["LEFT JOIN"], join_conditions = join_conditions,
                where= ["{0} is null".format(self.key[0])])

        changed = self.select(values = source_values, source = tables,
                  join_types = ["JOIN"], join_conditions = join_conditions,
                  where= where_conditions)

        self.update(
              ["{0}".format(self.valid_column),"{0}".format(self.end_column)],
              ["1","getdate()"],

            )


  def select_valid(self,values = ['*'],sources = [],join_types = []
    ,join_conditions = [], where = [], order = [], group = []):

    return self.select(values = values,sources = sources,join_types = join_types
      ,join_conditions = join_conditions,
      where = where.append("{0} = 1".format(self.valid_column) ), order = [], group = [])