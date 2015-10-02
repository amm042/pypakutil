import ConfigParser
import random
import os.path
import json
import logging

class Configuration(object):

    def __init__(self, filename = ""):
        self.logger = logging.getLogger("SensorAPI_API")
        if self.logger.isEnabledFor(logging.DEBUG):
            self.logger.debug("Starting to read configuration file")
        if len(filename) > 0:
            if self.logger.isEnabledFor(logging.DEBUG):
                self.logger.debug("Using given filename: {0}".format(filename))
            self.loadConfiguration(filename)
            return
        elif os.path.isfile("master.json"):
            if self.logger.isEnabledFor(logging.DEBUG):
                self.logger.debug("Using default master.json")
            self.loadConfiguration("master.json")
        else:
            if self.logger.isEnabledFor(logging.DEBUG):
                self.logger.debug("Creating default master.json")
            self.__cfg = {}
            self.__cfg["batchEnabled"] = False
            self.__cfg["host"] = "localhost"
            self.__cfg["port"] = "4242"
            self.__cfg["tags"] = {}
            self.saveConfiguration()

        self.logger.info("Configuration loaded. Host: {0}, Port: {1}".format(self.__cfg["host"], self.__cfg["port"]))


    def saveConfiguration(self, filename = ""):
        if len(filename) == 0:
            filename = "master.json"    # create a master json in the main folder
        file = open(filename, "w")
        file.write(json.dump(self.__cfg))
        file.close()
        if self.logger.isEnabledFor(logging.DEBUG):
            self.logger.debug("Configuration saved at {0}".format(filename))
       
    def getHost(self):
        return self.__cfg["host"]

    def getPort(self):
        return int(self.__cfg["port"])

    def loadConfiguration(self, filename):  
        self.__cfg = json.load(open(filename, 'r'))
        if self.logger.isEnabledFor(logging.DEBUG):
            self.logger.debug("Configuration loaded from {0}".format(filename))

    #def addTag(self, tagName):
    #    self.__cfg["tags"][tagName] = ""

    #def updateTag(self, tagName, value):
    #    self.__cfg["tags"][tagName] = value

    #def removeTag(self, tagName):
    #    if self.__cfg.has_key(tagName):
    #        self.__cfg["tags"].pop(tagName)
    #def hasTag(self, tagName):
    #    return tagName in self.__cfg["tags"]

