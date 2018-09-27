from Table import Table
from DMS import DMS_TYPE

class Dimension(Table):

  def __init__(self,dms_type,name,columns,key = [],query = [], virtual = False):
    Table.__init__(self,dms_type,name,columns,key = key, query = query, virtual= virtual)

  def update(self,values,data,source = [],conditions = []):
    raise "Type 1 SCDimensions cannot be updated"
