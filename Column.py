
class Column:
  
    def __init__(self, name, data_type, is_null=True, is_autonumber=False,
                 foreign_key_table_name='', foreign_key_column=None, alias=''):
        self.name = name
        self.container_name = ''
        self.alias = alias
        self.data_type = data_type
        self.is_null = is_null
        self.is_autonumber =  is_autonumber
        self.foreign_key_table_name = foreign_key_table_name
        self.foreign_key_column = foreign_key_column


    def equals(self, dbms, target):
        if isinstance(target, Column):
            return dbms.equals(self.container_name + '.' + self.name, target.container_name +'.'+ target.name)
        return dbms.equals(self.container_name + '.' + self.name, target)
    
    def different(self, dbms, target):
        if isinstance(target, Column):
            return dbms.different(self.container_name + '.' + self.name, target.container_name +'.'+ target.name)
        return dbms.different(self.container_name + '.' + self.name, target)

    def is_grater(self, dbms, target):
        if isinstance(target, Column):
            return dbms.is_grater(self.container_name + '.' + self.name, target.container_name +'.'+ target.name)
        return dbms.is_grater(self.container_name + '.' + self.name, target)

    def is_grater_or_equal(self, dbms, target):
        if isinstance(target, Column):
            return dbms.is_grater_or_equal(self.container_name + '.' + self.name, target.container_name +'.'+ target.name)
        return dbms.is_grater_or_equal(self.container_name + '.' + self.name, target)

    def is_less(self, dbms, target):
        if isinstance(target, Column):
            return dbms.is_less(self.container_name + '.' + self.name, target.container_name +'.'+ target.name)
        return dbms.is_less(self.container_name + '.' + self.name, target)

    def is_less_or_equal(self, dbms, target):
        if isinstance(target, Column):
            return dbms.is_less_or_equal(self.container_name + '.' + self.name, target.container_name +'.'+ target.name)
        return dbms.is_less_or_equal(self.container_name + '.' + self.name, target)

    def between(self, dbms, left_limit, right_limit):
        if isinstance(left_limit, Column) and isinstance(right_limit, Column):
            return dbms.between(
                self.container_name + '.' + self.name, 
                left_limit.container_name + '.' + left_limit.name if isinstance(left_limit, Column) else left_limit,
                right_limit.container_name + '.' + right_limit.name if isinstance(right_limit, Column) else right_limit
                )


    def null(self, dbms):
        return dbms.is_null(self.container_name + '.' + self.name, True)

    def not_null(self, dbms):
        return dbms.is_null(self.container_name + '.' + self.name, False)

    def in_(self, dbms, query):
        return dbms.in_(self.container_name + '.' + self.name, query.code(), True)

    def not_in(self, dbms, query):
        return dbms.in_(self.container_name + '.' + self.name, query.code(), False)

    def count(self, dbms, distinct):
        return dbms.count(self.container_name + '.' + self.name, distinct)

    def sum(self, dbms):
        return dbms.sum(self.container_name + '.' + self.name)

    def max(self, dbms):
        return dbms.max(self.container_name + '.' + self.name)

    def min(self, dbms):
        return dbms.min(self.container_name + '.' + self.name)