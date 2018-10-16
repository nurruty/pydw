
class Column:
  
    def __init__(self, name, data_type, is_null=True, is_autonumber=False,
                 foreign_key_table_name='', foreign_key_column=None, alias='', data=''):
        self.name = name
        self.data = data if data else name
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

    def equals(self, target, full_name=True):
        if isinstance(target, Column):
            if full_name:
                return self.dbms.equals(self.container_name + '.' + self.name, target.container_name +'.'+ target.name)
            return self.dbms.equals(self.name, target.container_name +'.'+ target.name)
        if full_name:
            return self.dbms.equals(self.container_name + '.' + self.name, target)
        return self.dbms.equals(self.name, target)

    def different(self, target, full_name=True):
        if isinstance(target, Column):
            if full_name:
                return self.dbms.different(self.container_name + '.' + self.name, target.container_name +'.'+ target.name)
            return self.dbms.different(self.name, target.container_name +'.'+ target.name)
        if full_name:
            return self.dbms.different(self.container_name + '.' + self.name, target)
        return self.dbms.different(self.name, target)

    def is_grater(self, target, full_name=True):
        if isinstance(target, Column):
            if full_name:
                return self.dbms.is_grater(self.container_name + '.' + self.name, target.container_name +'.'+ target.name)
            return self.dbms.is_grater(self.name, target.container_name +'.'+ target.name)
        if full_name:
            return self.dbms.is_grater(self.container_name + '.' + self.name, target)
        return self.dbms.is_grater(self.name, target)

    def is_grater_or_equal(self, target, full_name=True):
        if isinstance(target, Column):
            if full_name:
                return self.dbms.is_grater_or_equal(self.container_name + '.' + self.name, target.container_name +'.'+ target.name)
            return self.dbms.is_grater_or_equal(self.name, target.container_name +'.'+ target.name)
        if full_name:
            return self.dbms.is_grater_or_equal(self.container_name + '.' + self.name, target)
        return self.dbms.is_grater_or_equal(self.name, target)

    def is_less(self, target, full_name=True):
        if isinstance(target, Column):
            if full_name:
                return self.dbms.is_less(self.container_name + '.' + self.name, target.container_name +'.'+ target.name)
            return self.dbms.is_less(self.name, target.container_name +'.'+ target.name)
        if full_name:
            return self.dbms.is_less(self.container_name + '.' + self.name, target)
        return self.dbms.is_less(self.name, target)

    def is_less_or_equal(self, target, full_name=True):
        if isinstance(target, Column):
            if full_name:
                return self.dbms.is_less_or_equal(self.container_name + '.' + self.name, target.container_name +'.'+ target.name)
            return self.dbms.is_less_or_equal(self.name, target.container_name +'.'+ target.name)
        if full_name:
            return self.dbms.is_less_or_equal(self.container_name + '.' + self.name, target)
        return self.dbms.is_less_or_equal(self.name, target)

    def between(self, left_limit, right_limit, full_name=True):
        if isinstance(left_limit, Column) and isinstance(right_limit, Column):
            if full_name:
                return self.dbms.between(
                    self.container_name + '.' + self.name,
                    left_limit.container_name + '.' + left_limit.name if isinstance(left_limit, Column) else left_limit,
                    right_limit.container_name + '.' + right_limit.name if isinstance(right_limit, Column) else right_limit
                    )
            return self.dbms.between(
                    self.name,
                    left_limit.container_name + '.' + left_limit.name if isinstance(left_limit, Column) else left_limit,
                    right_limit.container_name + '.' + right_limit.name if isinstance(right_limit, Column) else right_limit
                    )


    def null(self, full_name=True):
        if full_name:
            return self.dbms.is_null(self.container_name + '.' + self.name, True)
        return self.dbms.is_null(self.name, True)

    def not_null(self, full_name=True):
        if full_name:
            return self.dbms.is_null(self.container_name + '.' + self.name, False)
        return self.dbms.is_null(self.name, False)

    def in_(self, query, full_name=True):
        if full_name:
            return self.dbms.in_(self.container_name + '.' + self.name, query.code(), True)
        return self.dbms.in_(self.name, query.code(), True)

    def not_in(self, query, full_name=True):
        if full_name:
            return self.dbms.in_(self.container_name + '.' + self.name, query.code(), False)
        return self.dbms.in_(self.name, query.code(), False)

    def count(self, distinct):
        return self.dbms.count(self.container_name + '.' + self.name, distinct)

    def sum(self):
        return self.dbms.sum(self.container_name + '.' + self.name)

    def max(self):
        return self.dbms.max(self.container_name + '.' + self.name)

    def min(self):
        return self.dbms.min(self.container_name + '.' + self.name)

    def trim(self):
        if self.dbms.is_char(self.data_type):
            self.data = self.dbms.trim(self.data)

    def set_alias(self, alias):
        self.alias = alias

    def empty_value(self):
        if self.dbms.is_numeric(self.data_type) or self.dbms.is_int(self.data_type) or self.dbms.is_decimal(self.data_type):
            return str(0)
        elif self.dbms.is_char(self.data_type):
            return "''"
        elif self.dbms.is_date(self.data_type):
            return self.dbms.empty_date()
        else:
            return self.dbms.null_value()