'''
Created on 2014-08-23

@author: Diego
'''
import os

import QueryCondition
import SliceDB
import SliceEnv
import SliceModule
import SliceQuery


class Usage(Exception):
    def __init__(self, msg):
        self.msg = msg


def main(argv=None):
    menu = """
    Please enter the character in front of the method you would like to use(Enter 'help' to display the menu again and q to quit the program.):
    a:Create database    (table name, count, column name/column_type pairs, index)
    b:Update Record      (database name, data values)
    c:Add record         (database name, data values)
    d:Delete Record      (database name, key value)
    e:Bulk load          (database name, upload file name)
    f:Display Join       (database 1, database 2, join column)
    g:Run Query          (table name, display_columns, condition)
    h:Report 1           (nothing required)
    i:Report 2           (nothing required)
    j:Print Databases    (nothing required)
    k:print Records      (database name)
    q:Exit               (nothing required)
    """
    print(menu)

    schemaElements = [SliceModule.SliceModule("cust", SliceModule.DataType.Int),
                      SliceModule.SliceModule("name", SliceModule.DataType.String),
                      SliceModule.SliceModule("age", SliceModule.DataType.Int),
                      SliceModule.SliceModule("phone", SliceModule.DataType.String),
                      SliceModule.SliceModule("address", SliceModule.DataType.String)]
    env = SliceEnv.SliceEnv()
    env.createDB("CustDB", schemaElements, "cust")

    sliceDB = env.openDB("CustDB")

    sliceRecord = sliceDB.createRecord();

    sliceRecord.setElement("name", "Joe Smith")
    sliceRecord.setElement("cust", 1)
    sliceRecord.setElement("age", 43)
    sliceRecord.setElement("address", "Montreal")

    sliceDB.set(sliceRecord)

    schemaElements = [SliceModule.SliceModule("order", SliceModule.DataType.Int),
                      SliceModule.SliceModule("cust", SliceModule.DataType.Int),
                      SliceModule.SliceModule("date", SliceModule.DataType.String),
                      SliceModule.SliceModule("total", SliceModule.DataType.Double)]
    envOther = SliceEnv.SliceEnv()
    envOther.createDB("SalesDB", schemaElements, "order")

    userInput = ""
    env = None
    sliceDB = None
    databaseName = ""
    sliceRecord = None
    env = SliceEnv.SliceEnv()
    while (userInput != "q"):
        try:
            userInput = input("input> ").strip()  # remove spaces.

            # Process options
            if userInput == 'q':  # Exit
                pass
            elif userInput == 'help':  # Help
                print(menu)
            elif userInput == 'a':
                print("Please enter the name of the database:")
                databaseName = input("> ").strip()  # remove spaces.
                print("Please enter the number of columnName|columnType pairs:")
                count = input("> ").strip()  # remove spaces.
                count = int(count)
                schemaElements = list()
                for index in range(count):
                    print("Please enter columnName|columnType pairs:")
                    pair = input("> ").strip()  # remove spaces.
                    schemaElements.append(str(pair))
                print("Please enter the index column(Choose From ", schemaElements, " ):")
                indexColumn = input("> ").strip()  # remove spaces.
                schemaElements = createDataBase(databaseName, schemaElements, indexColumn)

                if indexColumn == "":
                    env.createDB(databaseName, schemaElements)
                else:
                    env.createDB(databaseName, schemaElements, indexColumn)
                sliceDB = env.openDB(databaseName)
                sliceDB.writeDataBase()  # write new database to disk.

            elif userInput == 'b':
                print("Please enter the name of the database you would like to update a SliceRecord:")
                databaseName = input("> ").strip()  # remove spaces.
                sliceDB = env.openDB(databaseName)
                if sliceDB is None:
                    pass
                else:
                    sliceRecord = sliceDB.createRecord();
                    schemaElements = sliceDB.getschemaElements
                    for element in schemaElements:
                        print("Enter a Value for:", element.columnName, " Data Type:", element.dataType)
                        value = input("> ").strip()  # remove spaces.

                        if value == "" and element.dataType == 1:
                            value = -1
                        if value == "" and element.dataType == 2:
                            value = -0.0

                        if element.dataType == 1:  # The expected type is Integer, so convert current field to integer.
                            value = int(value)
                        elif element.dataType == 2:  # The expected type is float, so convert current field to float.
                            value = float(value)
                        elif element.dataType == 3:  # The expected type is string, don't convert since it's already a string..
                            value = str(value)
                        sliceRecord.setElement(element.columnName, value)

                    sliceDB.set(sliceRecord)
                    sliceDB.writeDataBase()

            elif userInput == 'c':
                print("Please enter the name of the database you would like to add a SliceRecord:")
                databaseName = input("> ").strip()  # remove spaces.
                sliceDB = env.openDB(databaseName)
                if sliceDB is None:
                    pass
                else:
                    sliceRecord = sliceDB.createRecord();
                    schemaElements = sliceDB.getschemaElements
                    for element in schemaElements:
                        print("Enter a Value for:", element.columnName, " Data Type:", element.dataType)
                        value = input("> ").strip()  # remove spaces.

                        if value == "" and element.dataType == 1:
                            value = -1
                        if value == "" and element.dataType == 2:
                            value = -0.0

                        if element.dataType == 1:  # The expected type is Integer, so convert current field to integer.
                            value = int(value)
                        elif element.dataType == 2:  # The expected type is float, so convert current field to float.
                            value = float(value)
                        elif element.dataType == 3:  # The expected type is string, don't convert since it's already a string..
                            value = str(value)
                        sliceRecord.setElement(element.columnName, value)

                    sliceDB.set(sliceRecord)
                    print("About to write to database.")
                    sliceDB.writeDataBase()

            elif userInput == 'd':
                print("Please enter the name of the database you would like to delete a SliceRecord:")
                databaseName = input("> ").strip()  # remove spaces.
                sliceDB = env.openDB(databaseName)
                if sliceDB is None:
                    pass
                else:
                    dataType = None
                    print("Please enter key Value of the value you wish to remove:")
                    value = input("> ").strip()  # remove spaces.
                    schemaElements = sliceDB.getschemaElements
                    indexColumn = sliceDB.getindexColumn
                    if indexColumn == "":
                        print("Can't perform delete operation since the index Column is empty.")
                        pass
                    else:
                        for element in schemaElements:
                            if element.columnName == indexColumn:
                                dataType = element.dataType
                        if dataType == 1:  # The expected type is Integer, so convert current field to integer.
                            value = int(value)
                        elif dataType == 2:  # The expected type is float, so convert current field to float.
                            value = float(value)
                        elif dataType == 3:  # The expected type is string, don't convert since it's already a string..
                            value = str(value)

                        # Delete record
                        sliceDB.deleteRecord(value)
                        sliceDB.writeDataBase()

            elif userInput == 'e':
                print("Please enter the name of the database:")
                databaseName = input("> ").strip()  # remove spaces.
                sliceDB = env.openDB(databaseName)
                if sliceDB is None:
                    pass
                else:
                    print("Please enter the path for the file:")
                    pathName = input("> ").strip()  # remove spaces.
                    sliceDB.load(pathName)
                    sliceDB.writeDataBase()

            elif userInput == 'f':
                print("Please enter the name of the local DataBase:")
                databaseName = input("> ").strip()  # remove spaces.
                sliceDB = env.openDB(databaseName)
                if sliceDB is None:
                    pass
                else:
                    print("Please enter the name of the other DataBase:")
                    databaseName2 = input("> ").strip()  # remove spaces.
                    otherSliceDB = env.openDB(databaseName2)
                    if otherSliceDB is None:
                        pass
                    else:
                        print("Please enter the column name for the join to take place:")
                        joinColumn = input("> ").strip()  # remove spaces.
                        sliceDB.join(otherSliceDB, joinColumn)

            elif userInput == 'g':
                print("Please enter the name of the Data Base you wish to perform queries on:")
                databaseName = input("> ").strip()  # remove spaces.
                sliceDB = env.openDB(databaseName)
                if sliceDB is None:
                    pass
                else:
                    print("Please enter the columns you would like to be displayed:")
                    columns = input("> ").strip()  # remove spaces.
                    print("Please enter the condition:")
                    conditions = input("> ").strip()  # remove spaces.

                    elementsCondition = conditions.split("|")
                    if elementsCondition[1] == "EQ":
                        elementsCondition[1] = QueryCondition.SliceOP.EQ
                    elif elementsCondition[1] == "GT":
                        elementsCondition[1] = QueryCondition.SliceOP.GT
                    elif elementsCondition[1] == "LT":
                        elementsCondition[1] = QueryCondition.SliceOP.LT
                    schemaElements = sliceDB.getschemaElements
                    dataType = None
                    for element in schemaElements:
                        if element.columnName == elementsCondition[0]:
                            dataType = element.dataType
                    if dataType == 1:
                        elementsCondition[2] = int(elementsCondition[2])
                    if dataType == 2:
                        elementsCondition[2] = float(elementsCondition[2])
                    if dataType == 3:
                        elementsCondition[2] = str(elementsCondition[2])

                    condition = QueryCondition.QueryCondition(elementsCondition[0], elementsCondition[1],
                                                              elementsCondition[2])
                    columnsArray = columns.split("|")
                    query = SliceQuery.SliceQuery(columnsArray, databaseName, condition)
                    SliceDB.SliceDB.queryDatabase(query);

            elif userInput == 'h':
                os.system("report1.exe")

            elif userInput == 'i':
                os.system("report2.exe")

            elif userInput == 'j':
                dataBases = env.getDataBases
                print("Databases:")
                for key in dataBases.keys():
                    print("    ", key)

            elif userInput == 'k':
                print("Please enter the name of the database you wish to display content:")
                databaseName = input("> ").strip()  # remove spaces.
                sliceDB = env.openDB(databaseName)
                if sliceDB is None:
                    pass
                else:
                    rowsPrint = sliceDB.getRows
                    for element in rowsPrint:
                        print(element.schemaElementsDict)

        except Exception  as err:
            print(err)
            pass


def createDataBase(databaseName, schema_elements, indexColumn):
    # parse elements
    readySchemaElements = []
    for element in schema_elements:
        tempArray = element.split('|')
        if tempArray[1] == "STRING" or tempArray[1] == "String" or tempArray[1] == "string":
            readySchemaElements.append(SliceModule.SliceModule(tempArray[0], SliceModule.DataType.String))
        elif tempArray[1] == "INT" or tempArray[1] == "Int" or tempArray[1] == "int":
            readySchemaElements.append(SliceModule.SliceModule(tempArray[0], SliceModule.DataType.Int))
        elif tempArray[1] == "DOUBLE" or tempArray[1] == "Double" or tempArray[1] == "double":
            readySchemaElements.append(SliceModule.SliceModule(tempArray[0], SliceModule.DataType.Double))

    return readySchemaElements


if __name__ == '__main__':
    main()
