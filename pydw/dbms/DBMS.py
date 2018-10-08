import abc
from abc import ABC
from enum import Enum

class DBMS_TYPE(Enum):
    SQL_SERVER = 1
    MYSQL = 2
    ORACLE = 3

class DBMS(ABC):

    @abc.abstractmethod
    def create_table(self, table_name, column_names, column_types, column_nullable, key, foreign_keys):
        pass

    def drop_table(self, table_name):
        pass
    
    @abc.abstractmethod
    def add_column(self, table_name, column_name, data_type):
        pass

    @abc.abstractmethod
    def drop_column(self, table_name, column_name):
        pass

    @abc.abstractmethod
    def alter_column(self, table_name, column_name, data_type):
        pass
    
    @abc.abstractmethod
    def truncate_table(self, table_name):
        pass

    @abc.abstractmethod
    def insert(self, table_name, values, source):
        pass
  
    @abc.abstractmethod
    def update(self, table_name, values, data, source, conditions):
        pass

    @abc.abstractmethod
    def delete(self, table_name, conditions):
        pass

    @abc.abstractmethod
    def select(self, values, sources, join_types, join_conditions, conditions, group, order, order_asc):
        pass

    @abc.abstractmethod
    def select_into(self, table_name, values, sources, join_types, join_conditions, where, group, order, order_asc):
        pass

    @abc.abstractmethod
    def create_procedure(self, name, code, params):
        pass

    @abc.abstractmethod
    def alter_procedure(self, name, code, params):
        pass

    @abc.abstractmethod
    def get_columns(self, database_name, table_name):
        pass
  
    @abc.abstractmethod
    def get_key(self, database_name, table_name):
        pass

    @abc.abstractmethod
    def create_temporary_table(self, table_name, column_names, column_types, column_nullable):
        pass

    @abc.abstractmethod
    def declare_variable(self, name, data_type, value):
        pass
    
    @abc.abstractmethod
    def set_variable(self, name, value):
        pass

    @abc.abstractmethod
    def equals(self, a, b):
        pass

    @abc.abstractmethod
    def different(self, a, b):
        pass

    @abc.abstractmethod
    def is_grater(self, a, b):
        pass

    @abc.abstractmethod
    def is_grater_or_equal(self, a, b):
        pass

    @abc.abstractmethod
    def is_less(self, a, b):
        pass
    
    @abc.abstractmethod
    def is_less_or_equal(self, a, b):
        pass

    @abc.abstractmethod
    def in_(self, element, source, is_in):
        pass

    @abc.abstractmethod
    def between(self, element, left_limit, right_limit):
        pass

    @abc.abstractmethod
    def is_null(self, element, null):
        pass

    @abc.abstractmethod
    def order_asc(self, asc):
        pass

    @abc.abstractmethod
    def union(self, sources):
        pass

    @abc.abstractmethod
    def count(self, element, distinct):
        pass

    @abc.abstractmethod
    def sum(self, element):
        pass

    @abc.abstractmethod
    def max(self, element):
        pass

    @abc.abstractmethod
    def min(self, element):
        pass   