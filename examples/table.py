import pakbus
from sensorTag import SensorTag

from API.SensorClient import *
import json
import requests
import sys

import logging
#logging.basicConfig(filename="PyPakLogging.log", level=logging.DEBUG)

# create a sensor client
#client = SensorClient()

import string
def make_follow_tag_rules(s):
    '''remove " ", "^", "%"'''
    s = string.replace(s, " ", "_")
    s = string.replace(s, "^", "")
    s = string.replace(s, "%", "Percent")
    return s

class Table:
    ''' This part does not require a connection to datalogger to work at this exact moment.
        All the information is stored in tabledef
    '''

    all_tag_metrics = []

    def __init__(self, name, tabledef, tableno):
        self.log = logging.getLogger(__name__)
        self.name = name
        self.tableno = tableno
        self.dic_of_sensorTags, self.record_tag = self.getSensorTags(tabledef)
        with open ("tag_metrics.txt", "w") as f:
            string = ""
            for metric in Table.all_tag_metrics:
                if (metric != Table.all_tag_metrics[-1]):
                    string += metric + "\n"
                else:
                    string += metric
            f.write(string)
        self.log.info( "Got all table information for Table: {}".format(name))


    def __repr__(self):
        return "Table Name: " + str(self.name) + "; Table No: " + str(self.tableno+1) + \
               "; Sensor Tags: " + str(self.dic_of_sensorTags)

    def getSensorTags(self, tabledef):
        dic_of_sensorTags = {}
        for fieldno in range(len(tabledef[self.tableno]['Fields'])):
            sensor_name = make_follow_tag_rules(tabledef[self.tableno]['Fields'][fieldno]['FieldName'])
            sensor_units = make_follow_tag_rules(tabledef[self.tableno]['Fields'][fieldno]['Units'])
            sensor_processing = make_follow_tag_rules(tabledef[self.tableno]['Fields'][fieldno]['Processing'])
            dic_of_sensorTags[sensor_name] = ""
            dic_of_sensorTags[sensor_name] = SensorTag(sensor_name, sensor_units, sensor_processing, self.name)
            if (self.name not in Table.all_tag_metrics):
                Table.all_tag_metrics += [self.name]
            #print dic_of_sensorTags
        record_tag = Tags(self.name)
        record_tag.addTag("sensor_name", "RecordNumber")
        return dic_of_sensorTags, record_tag
