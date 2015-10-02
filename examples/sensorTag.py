from Client import *

import logging
#logging.basicConfig(filename="PyPakLogging.log", level=logging.DEBUG)

##import pickle

class SensorTag:

    list_of_all_tags = []

    def __init__(self, name, units, processing, table_name):
        self.log = logging.getLogger(__name__)
        self.name = name
        self.units = units
        self.processing = processing
        self.tag = self.makeTag(table_name)
        SensorTag.list_of_all_tags += [self.tag]
##        pickle.dump(SensorTag.list_of_all_tags, open("allTags.txt", "w"))
        self.log.info("Made tag for sensors with Table: {} for sensor {}".format(table_name,
                                                                                 self.name))

    def __repr__(self):
        return "Sensor Name: " + str(self.name) + "; Sensor Units: " + str(self.units) + \
               "; Sensor Process: " + str(self.processing)


    def makeTag(self, table_name):
        tag = Tags(table_name)
        if (self.name == None or self.name == ""):
            tag.addTag("sensor_name", "Name")
            self.name = "Name"
        else:
            tag.addTag("sensor_name", self.name)
        if (self.units == None or self.units == ""):
            tag.addTag("sensor_units", "Units")
            self.units = "Units"
        else:
            tag.addTag("sensor_units", self.units)
        if (self.processing == None or self.processing == ""):
            tag.addTag("sensor_processing", "Processing")
            self.processing = "Processing"
        else:
            tag.addTag("sensor_processing", self.processing)
        return tag
