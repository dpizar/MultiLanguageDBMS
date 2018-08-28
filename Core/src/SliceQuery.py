"""
Created on Aug 25, 2014

@author: Diego
"""


class SliceQuery:
    """Constructor: sets the name for the column and it's data Type(INT,Double or String)"""

    def __init__(self, columns, databaseName, condition):
        self._displayedColumns = columns  # will contain an array of strings. Each specifying the columns to be displayed.
        self._databaseName = databaseName  # will contain the name of the database where the query will be ran.
        self._condition = condition  # will contain an object of type QueryCondition

    """Setters and Getters """

    @property
    def displayedColumns(self):
        return self._displayedColumns

    @property
    def databaseName(self):
        return self._databaseName

    @property
    def condition(self):
        return self._condition
