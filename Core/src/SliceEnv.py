'''
Created on 2014-08-23

@author: Diego
'''
import SliceDB


class SliceEnv:
    # static variable that will contain the databases.(databaseName,dataBaseObject)
    dataBases = dict()

    """ empty constructor"""

    def __init__(self):
        pass

    """ Create a new Database """

    def createDB(self, databaseName, schemaElements, indexColumn=""):
        # create a new data base
        SliceEnv.dataBases[databaseName] = SliceDB.SliceDB(databaseName, schemaElements, indexColumn)

    """ Open an existing database """

    def openDB(self, databaseName):
        if databaseName in SliceEnv.dataBases:
            return SliceEnv.dataBases.get(databaseName)
        else:
            print("The Data Base name: ", databaseName, " does not exist. Please enter a valid name.")
            return None

    """Setters and Getters """

    @property
    def getDataBases(self):
        return SliceEnv.dataBases
