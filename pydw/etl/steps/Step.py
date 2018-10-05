import abc
from abc import ABC


class Step(ABC):
# This abstract class represents different type of steps
# An step must have an input, which might be the result of
# other step. The data param must be a dict, wich will be
# used in each instanciable class. Finally it returns an output
# and its executable code
  def __init__(self, dbms, name, input_, data):

    self.name = name
    self.input = input_
    self.dbms = dbms
    self.data = data

  @abc.abstractmethod
  # This method return the SQL code wich enables the
  # the execution of this step
  def code(self):
    pass


  @abc.abstractmethod
  # This method returns the objects wich can be input of other
  # step
  def output(self):
    pass