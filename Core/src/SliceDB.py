'''
Created on Aug 24, 2014

@author: Diego
'''
import SliceEnv
import SliceModule
import SliceRecord


class SliceDB:
    """Constructor: initializes an object with the database name, the schema columns and the column that will be used as a index for retrival"""

    def __init__(self, databaseName, schemaElements, indexColumn=""):  # indexColumn is optional
        self.databaseName = databaseName  # Will be a string
        self.schemaElements = schemaElements  # will be an array of SliceModules
        self.indexColumn = indexColumn  # will be a String
        self.rows = []  # will contain an array of SliceRecords

    """Will create a new  record object to hold the values for a row in the database. It will return a reference to the created SliceRecord """

    def createRecord(self):
        #  Create a new Record Object and send the current schema elements so the object knows the elements.
        dataBaseSliceRecord = SliceRecord.SliceRecord(self.schemaElements);
        return dataBaseSliceRecord

    '''will create a new row in the database. if an index was provided to the database and the index value already exists then the that row is updated otherwise
    we add a new record. Further, in order to update a row the database has to contain an index.
    '''

    def set(self, sliceRecord):
        #  check if the Current database contains an index column
        if self.indexColumn != "":
            intTemp = self.check(sliceRecord)
            if intTemp >= 0:  # if true an element with the same key value already exists and needs to be updated.
                self.updateRow(intTemp, sliceRecord)
            else:  # record doesn't need to be modified. Add new record
                self.rows.append(sliceRecord)
        else:  # if there is no index just append the new row.
            self.rows.append(sliceRecord)

    def deleteRecord(self, keyValue):
        #  check if the Current database contains an index column
        if self.indexColumn == "":
            print("The current Database doesn't contain an index Column. The delete operation can't be executed.")
            return
        else:
            for element in self.rows:
                dictTemp = element.schemaElementsDict
                if dictTemp[self.indexColumn] == keyValue:
                    self.rows.remove(element)

    """ Check to see if the current record already exists. Return the index of the element in the array rows if found otherwise it will return -1"""

    def check(self, sliceRecord):
        recordTemp = sliceRecord.schemaElementsDict  # get dictionary
        for element in self.rows:
            elemDict = element.schemaElementsDict
            if elemDict[self.indexColumn] == recordTemp[
                self.indexColumn]:  # compare row values for the indexColumn and make sure it is not the same object.
                # print (elemDict[self.indexColumn], " :" ,recordTemp[self.indexColumn])
                # print ("Record with that index:",elemDict[self.indexColumn] ," Already Exits")
                return self.rows.index(element)  # return the index of respective row in the rows array.
        return -1

    """ Update the values for a current row"""

    def updateRow(self, index, sliceRecord):

        # get objet dictionary
        sliceRecordTemp = self.rows[index]
        dictTemp = sliceRecordTemp.schemaElementsDict
        # print("Record:",self.rows[index].schemaElementsDict," Has been Updated with Record:",sliceRecord.schemaElementsDict)
        # update to row to new values.
        self.rows[index] = sliceRecord

    """load Data bulk from text file, delete all previews Slice records in the database"""

    def load(self, fileName):
        self.rows.clear()  # remove all previews Slice records in the database.
        arrayFile = []  # will contain strings for each line of a text file being read.
        try:
            file = open(fileName, 'r')  # open file for reading only.
        except IOError:
            print("Error: can't find file or read data")
            return
        arrayFile = file.readlines()  # readlines() will return an array of string each string for a new line
        self.parseInsert(arrayFile)  # Create Data.

        file.close()

        print("Number Of Rows Impoorted:", len(self.rows))

    """Parse each record and put it in the currently opened database """

    def parseInsert(self, dataArray):
        for element in dataArray:

            sliceRecord = self.createRecord();  # create new record.

            element = element.rstrip()  # remove all white spaces and new lines.

            fieldArray = element.split(
                '|')  # Separate each record with the '|' delimeter, and put each field in a different string.
            if len(fieldArray) == len(
                    self.schemaElements):  # check if the number of elements in each record is the same as the expected in the schema.
                currentIndex = 0  # will keep track of the current position from the field of a record being analized.
                for field in fieldArray:
                    schemaDataType = self.schemaElements[
                        currentIndex].dataType  # get the proper data type from the schema.
                    if schemaDataType == 1:  # The expected type is Integer, so convert current field to integer.
                        typeField = int(field)
                    elif schemaDataType == 2:  # The expected type is float, so convert current field to float.
                        typeField = float(field)
                    elif schemaDataType == 3:  # The expected type is string, don't convert since it's already a string..
                        typeField = str(field)

                    sliceRecord.setElement(self.schemaElements[currentIndex].columnName,
                                           typeField)  # Put Element in sliceRecord

                    currentIndex = currentIndex + 1  # update index to point to the next field in the record.
            else:
                print("The number of elements expected ,per row, for the current schema is:", len(self.schemaElements),
                      " And it does not match the number in the data file which is: ", len(fieldArray),
                      " Error could not import data!!!")
                break
            self.set(sliceRecord)

    """get: will look for a record in the current database and return it if it finds it. The search is done on the specified index column; if one was not declared then the search can't be done"""

    def get(self, indexColumnValue):
        # check if an index column exist
        if self.indexColumn == "":
            print("An index column was not specified in the database: ", self.databaseName,
                  " The search can not be done!!")
            return None
        # Make sure the passed argument has the same data type as the index Column.
        sliceRecord = self.rows[0].schemaElementsDict
        if type(sliceRecord[self.indexColumn]) != type(indexColumnValue):
            print("The index column has data type:", type(self.indexColumn), " But the argument sent is of type: ",
                  type(indexColumnValue), ". Please send a matching type")
            return
        # an index column exist so search for the index.
        for element in self.rows:
            dictionary = element.schemaElementsDict
            if dictionary[self.indexColumn] == indexColumnValue:  # if true a matching element has been found
                return element
        return None  # return and empty object if nothing is found

    """perform a natural join for two databases(Tables)"""

    def join(self, dataBase, joinColumn):
        # check if both databases contain the joinColumn and if they  have the same dataType.
        boolCheck = self.checkJoinCompability(dataBase, joinColumn)
        if (boolCheck == False):  # can't perform join so just return.
            return
        repeatedNames = list()  # will hold an array of names already used in the local database(Table)
        schemaElements = []
        # mark all columns with the same name.
        for selfElement in self.schemaElements:
            for otherElement in dataBase.schemaElements:
                if selfElement.columnName == otherElement.columnName:  # if another column with the same name has been found in the second table mark it, so it can be changed later.
                    repeatedNames.append(selfElement.columnName)

        # create the new schema for the join
        for element in self.schemaElements:  # Local Database
            schemaElements.append(SliceModule.SliceModule(element.columnName, element.dataType))

        for element in dataBase.schemaElements:  # other Database
            if element.columnName == joinColumn:  # don't add another joinColumn to the new database
                continue
            elif element.columnName in repeatedNames:  # if the current column Name already exists, change it
                newName = dataBase.databaseName + "_" + element.columnName
                schemaElements.append(SliceModule.SliceModule(newName, element.dataType))
            else:  # the column is unique so add as is.
                schemaElements.append(SliceModule.SliceModule(element.columnName, element.dataType))

        # create the new join database.
        env = SliceEnv.SliceEnv()
        newDataBaseName = self.databaseName + dataBase.databaseName
        env.createDB(newDataBaseName, schemaElements)  # Create the new database without an indexColumn.
        # open the new DataBase
        newSliceDB = env.openDB(newDataBaseName)

        # Join the two databases and store the corresponding data in the newly created database.
        for element in self.rows:
            dict1 = element.schemaElementsDict
            for otherElement in dataBase.rows:
                dict2 = otherElement.schemaElementsDict
                if dict1[joinColumn] == dict2[
                    joinColumn]:  # if the joinColumn values from both databases are the same then create a new record for the new database.
                    # create new record for the join database
                    newSliceRecord = newSliceDB.createRecord();
                    for key in dict1.keys():  # copy values from the local database.
                        newSliceRecord.setElement(key, dict1[key])
                    for key in dict2.keys():  # copy only necessary values from other database.
                        if key != joinColumn:  # Don't copy the joinClomun value from the other database since we already have the one from the local database and that column no longer exists.
                            if key in repeatedNames:  # if the name from the new database was changed, then use the correct column name.
                                newKey = dataBase.databaseName + "_" + key
                                newSliceRecord.setElement(newKey, dict2[key])
                            else:  # the name is still the same.
                                newSliceRecord.setElement(key, dict2[key])

                    newSliceDB.set(newSliceRecord)  # save the new SliceRecord in join database.

        newSliceDB.writeDataBase()  # write new database to disk.
        # newSliceDB.printDatabase();

    """check if databases are compatible for doing a natural join on the joinColumn column"""

    def checkJoinCompability(self, dataBase, joinColumn):
        db1Type = None
        db2Type = None
        # find column 'joinColumn'
        for element in self.schemaElements:
            if (element.columnName == joinColumn):
                db1Type = element.dataType
        for element in dataBase.schemaElements:
            if (element.columnName == joinColumn):
                db2Type = element.dataType
        if db1Type == db2Type:
            return True
        else:
            print("Join can't be perform on data bases:", self.databaseName, " and ", dataBase.databaseName,
                  ". Since one of them or neither contains column:", joinColumn)
            return False

    """Write all record in the current Database to Disk """

    def writeDataBase(self):
        # print("entered function To write:")
        databaseFileName = self.databaseName + ".slc"
        # print("Will Write to database:",databaseFileName)
        file = open(databaseFileName, 'w')
        # print("Created File:",databaseFileName)
        for element in self.rows:
            record = element.schemaElementsDict
            recordString = ""
            for key in record.keys():
                recordString = recordString + str(record[key]) + "|"
            # Format String
            recordString = recordString[:-1]
            file.write(recordString)
            file.write("\n")

        file.close()

    # def appendToDataBase(self):
    """ Print records in a database."""

    def printDatabase(self):
        for element in self.rows:
            print(element.schemaElementsDict)

    """Setters and Getters """

    @property
    def getRows(self):
        return self.rows

    @property
    def getschemaElements(self):
        return self.schemaElements

    @property
    def getindexColumn(self):
        return self.indexColumn

    @staticmethod
    def queryDatabase(sliceQuery):
        # open the proper database
        queryEnv = SliceEnv.SliceEnv()
        queryDB = queryEnv.openDB(sliceQuery.databaseName)
        # check if given columns exist
        columnsToBeDisplayed = sliceQuery.displayedColumns
        elements = len(columnsToBeDisplayed)  # get number of columns that will be displayed.
        elemSoFar = 0
        for element in columnsToBeDisplayed:
            for schemaElement in queryDB.schemaElements:
                if element == schemaElement.columnName:
                    elemSoFar = elemSoFar + 1
        if (elemSoFar != elements):  # if they are equal then all the columns exist.
            print("One or more of the columns especified do not exist on the given DB. The query can not be executed.")
            return
        # Make sure the given literal has the same type as the column to which it will be compared agaisnt.

        queryCondition = sliceQuery.condition
        """
        for element in queryDB.schemaElements:
            if element.columnName == queryCondition.column:
                if element.dataType == type(queryCondition.literal):
                    boolComp=True
                    break
        if boolComp == False:
            print("Column data type and literal don't match. Can not execute query.")
            return
        """
        boolComp = False
        # Execute the query
        for element in queryDB.rows:
            dictionary = element.schemaElementsDict
            if queryCondition.equalityOperator == 1:
                if dictionary[queryCondition.column] == queryCondition.literal:
                    for i in columnsToBeDisplayed:
                        print(i, ":", dictionary[i], " ", end='')
                        boolComp = True
            elif queryCondition.equalityOperator == 2:
                if dictionary[queryCondition.column] > queryCondition.literal:
                    for i in columnsToBeDisplayed:
                        print(i, ":", dictionary[i], " ", end=' ')
                        boolComp = True
            elif queryCondition.equalityOperator == 3:
                if dictionary[queryCondition.column] < queryCondition.literal:
                    for i in columnsToBeDisplayed:
                        print(i, ":", dictionary[i], " ", end='')
                        boolComp = True
            if boolComp == True:
                print()
            boolComp = False
