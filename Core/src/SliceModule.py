"""
Class SliceModule specifies an object describing the name and data type for a column.
Created on 2014-08-24

@author: Diego
"""


from enum import IntEnum


class DataType(IntEnum):
    Int = 1
    Double = 2
    String = 3


class SliceModule:
 
    """Constructor: sets the name for the column and it's data Type(INT,Double or String)""" 
    def __init__(self, columnName, dataType):
        
        self._columnName= columnName
        # check that the correct data type is being used.If not assign a String value.
        if type(dataType) is DataType :
            self._dataType= dataType
        else:
            self._dataType = DataType.String
            
    """Setters and Getters """
    @property
    def columnName(self):
        return self._columnName
                
    @property
    def dataType(self):
        return self._dataType