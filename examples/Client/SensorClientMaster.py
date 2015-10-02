from API import *
from SensorClient import *
import ConfigParser
from threading import Timer

class ClientMaster(object):
    # UNDER DEVELOPMENT
    def __init__(self):
        self.__clients = []
        return

    def addClient(self, client):
        if type(client) is not SensorClient:
            return
        self.__clients.append(client)

    def removeClient(self, client):
        self.__clients.remove(client)


    
    


    #def batchingForever(self):
    #    self.__isBatching = True
    #    self.batch()
    #    self.__timer = Timer(self.batchPeriod, self.batchingForever())
    #    self.__timer.start()

    def stopBatching(self):
        if self.__isBatching:
            self.__timer.cancel()