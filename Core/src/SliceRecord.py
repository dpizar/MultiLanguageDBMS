"""
Created on Aug 24, 2014

@author: Diego
"""


class SliceRecord:
    """Constructor: initializes an object with the database name, the schema columns and the column that will be used as a index for retrival"""

    def __init__(self, schemaElements):
        # declare instance varibles
        self._schemaElementsDict = dict()  # will contain the values for each column from the parameter schema(schemaElements)
        # Set all dictionary variables to their default value depending on the data type.
        for element in schemaElements:
            # print(element.columnName, element.dataType)
            if element.dataType == 1:  # if the element is an integer
                self._schemaElementsDict[element.columnName] = -1
                # print(self._schemaElementsDict[element.columnName])
            elif element.dataType == 2:  # if the element is a double or float in python.
                self._schemaElementsDict[element.columnName] = 0.0
            elif element.dataType == 3:  # if the element is a String
                self._schemaElementsDict[element.columnName] = ""
                # print(self._schemaElementsDict[element.columnName])

    """setElement will assign 'value' into the proper column by using the name of the column sent('columnName')"""

    def setElement(self, columnName, value):
        # Check if the column name exist, if it does than add the corresponding value.
        if columnName in self._schemaElementsDict:
            if type(self._schemaElementsDict[columnName]) == type(
                    value):  # check that the value sent has the same datatype as the one expected in the schema column.
                self._schemaElementsDict[columnName] = value
            else:
                print(
                    "The data type sent does not match the data type specified in the current schema column: Sent Type: ",
                    type(value), " Column Type: ", type(self._schemaElementsDict[columnName]))
        else:
            print("The column: ", columnName, " Does not exist. Please enter a valid column name.");

    def getItemByColumnName(self, columnName):
        return self._schemaElementsDict[columnName]

    """Setters and Getters """

    @property
    def schemaElementsDict(self):
        return self._schemaElementsDict
