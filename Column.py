
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
        self.dbms = None


    def get_full_name(self):
        return self.container_name + '.' + self.name + ' ' + self.alias

    def equals(self, target):
        if isinstance(target, Column):
            return self.dbms.equals(self.container_name + '.' + self.name, target.container_name +'.'+ target.name)
        return self.dbms.equals(self.container_name + '.' + self.name, target)
    
    def different(self, target):
        if isinstance(target, Column):
            return self.dbms.different(self.container_name + '.' + self.name, target.container_name +'.'+ target.name)
        return self.dbms.different(self.container_name + '.' + self.name, target)

    def is_grater(self, target):
        if isinstance(target, Column):
            return self.dbms.is_grater(self.container_name + '.' + self.name, target.container_name +'.'+ target.name)
        return self.dbms.is_grater(self.container_name + '.' + self.name, target)

    def is_grater_or_equal(self, target):
        if isinstance(target, Column):
            return self.dbms.is_grater_or_equal(self.container_name + '.' + self.name, target.container_name +'.'+ target.name)
        return self.dbms.is_grater_or_equal(self.container_name + '.' + self.name, target)

    def is_less(self, target):
        if isinstance(target, Column):
            return self.dbms.is_less(self.container_name + '.' + self.name, target.container_name +'.'+ target.name)
        return self.dbms.is_less(self.container_name + '.' + self.name, target)

    def is_less_or_equal(self, target):
        if isinstance(target, Column):
            return self.dbms.is_less_or_equal(self.container_name + '.' + self.name, target.container_name +'.'+ target.name)
        return self.dbms.is_less_or_equal(self.container_name + '.' + self.name, target)

    def between(self, left_limit, right_limit):
        if isinstance(left_limit, Column) and isinstance(right_limit, Column):
            return self.dbms.between(
                self.container_name + '.' + self.name, 
                left_limit.container_name + '.' + left_limit.name if isinstance(left_limit, Column) else left_limit,
                right_limit.container_name + '.' + right_limit.name if isinstance(right_limit, Column) else right_limit
                )


    def null(self):
        return self.dbms.is_null(self.container_name + '.' + self.name, True)

    def not_null(self):
        return self.dbms.is_null(self.container_name + '.' + self.name, False)

    def in_(self, query):
        return self.dbms.in_(self.container_name + '.' + self.name, query.code(), True)

    def not_in(self, query):
        return self.dbms.in_(self.container_name + '.' + self.name, query.code(), False)

    def count(self, distinct):
        return self.dbms.count(self.container_name + '.' + self.name, distinct)

    def sum(self):
        return self.dbms.sum(self.container_name + '.' + self.name)

    def max(self):
        return self.dbms.max(self.container_name + '.' + self.name)

    def min(self):
        return self.dbms.min(self.container_name + '.' + self.name)