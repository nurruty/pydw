import abc
from enum import Enum

class DMS_TYPE(Enum):
  SQL_SERVER = 1
  MYSQL = 2
  ORACLE = 3

class DMS:
  
  def __init__(self,dms_type):
    self.dms_type = dms_type

  @abc.abstractmethod
  def insert(self,target,values,source):
    pass
  
  @abc.abstractmethod
  def update(self,target,values,data,source,conditions):
    pass

  @abc.abstractmethod
  def delete(self,target,conditions):
    pass

  @abc.abstractmethod
  def select(self,value,sources,join_types,join_conditions,conditions,order):
    pass

  @abc.abstractmethod
  def get_columns(self,database_name,table_name):
    pass
  
  @abc.abstractmethod
  def get_key(self,database_name,table_name):
    pass