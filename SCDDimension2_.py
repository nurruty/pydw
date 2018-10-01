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

    def update_scd2(self,source,join_conditions = [], where =[], audited_columns_names = []):
        
        (statements,new_var) = self.dms.temporay_table(
            name = "new",
            column_names = self.get_columns_names(),
            column_types = self.get_columns_types(),
            column_nullable = self.get_columns_nullable()
        )

        new_table = self.copy()
        new_table.name = new_var

        (aux, changed_var) = self.dms.temporay_table(
            name = "changed",
            column_names = self.get_columns_names(),
            column_types = self.get_columns_types(),
            column_nullable = self.get_columns_nullable()
        )

        changed_table = self.copy()
        changed_table.name = changed_var

        statements += aux

        query = Query(
            tables = [self.source],
            join_types = [self.dbms.left_join],
            join_conditions = join_conditions,
            where = ["{0} is null".format(self.key[0])] + where
        )


        query.add_columns([Column("1",self.dms.number(1),False),
                          Column(self.dms.today(),self.dms.number(1),False)])

        statements += self.dms.insert(
            target = new_var,
            value = self.get_columns_names(),
            source = query.code()
        )

        query = Query(
            columns = map(lambda v: v[0] if v[0].name in audited_columns_names else v[1]
                    , zip(self.columns,source.columns)),
            tables = [self,source],
            join_types = ["JOIN"]
            where = [self.dms.different(t,s) for (t,s) in zip(self.columns),source.columns) if t.name in audited_columns_names]
        )

        statements += self.dms.insert(
            target = changed_var,
            values = self.get_columns_names(),
            source = query.code()
        )

        query = Query(
            columns = [changed_table.key],
            tables = [changed_table]
        )

        statements += self.update(
            columns = [self.valid_column.name,target.end_column.name],
            data = ['0',self.dms.today()],
            where = [self.dms.where_in(target.key,query.code,True)]
        )

        statements += self.insert(
            values = target.get_columns_names()
            source = (Query(tables = [new_table]).union(Query(tables = [changed_table]))).code()
        )

        return statements