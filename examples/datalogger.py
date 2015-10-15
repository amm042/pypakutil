import pakbus
from table import Table
import os

#from API import *
import json
import requests
import sys

import time

import logging
import json
import datetime

import sqlite3
#logging.basicConfig(filename="PyPakLogging.log", level=logging.DEBUG)

#log = logging.getLogger("DataLogger")


# create a sensor client
#client = SensorClient()


class tblcache:
    """need to figure out a way to cache table defs"""
    def __init__(self, filename='tablecache.json'):

        if os.path.exists(filename):         
            with open(filename, 'r') as f:            
                self.cache = json.loads(f.read())                
        else:
            self.cache = {} 
            
    def add(self, metric, id, tabledef):
        "metric is the datalogger name and id is the pakbus id used to match loggers to our cache"
        
        
        
        
class DataLogger:
    
    #def __init__(self, number, socket, my_node_id, metric_id, pakbus_id):
    def __init__(self, db, socket, my_node_id, metric_id, pakbus_id, 
                 data_callback = None,
                 record_callback = None,
                 #wu_object = None,
                 security_code = 0x1111):
        """
        db is the sqlite db to use to store the last record numbers
        
        the data_callback is called for each time series (eg for openTSDB)
        the record callback is called once for each row in the datalogger's table 
        
        """
        #self.number = number

        self.log = logging.getLogger(__name__)
        
        self.db = sqlite3.connect(db)

        self.my_node_id = my_node_id
        self.metric_id = metric_id
        self.pakbus_id = pakbus_id
        self.security_code = security_code
        self.data_callback = data_callback
        self.record_callback = record_callback
        #self.wu_object = wu_object
        
        self.log.debug("Getting serial number for {}@{}".format(metric_id, pakbus_id))

        self.serno = pakbus.get_cr1000_serial(socket,
                                 self.pakbus_id,
                                 self.my_node_id,
                                 self.security_code)
        self.log.info("{}@{} has serial number {}".format(metric_id, pakbus_id, self.serno))
          
        self.log.debug("Getting table defs")
        self.FileData, RespCode = pakbus.fileupload(socket, 
                                          self.pakbus_id, 
                                          self.my_node_id, 
                                          FileName = '.TDF',
                                          SecurityCode = self.security_code)
        
        # todo, check resp code!
        self.log.debug(" -- table def RespCode was: {}".format(RespCode))
        #self.log.debug("Filedata = {}".format(self.FileData))
        
        self.tabledef = pakbus.parse_tabledef(self.FileData)

        self.log.debug("tabledef = {}".format(self.tabledef))

        self.list_of_tables = self.getTables(socket, 3)
        
        self.last_collect = {"ts": None,
                             "NextRecNbr": None}
        
        self.check_db()
        
    def check_db(self):
        c = self.db.cursor()
        
        sql =  "CREATE TABLE IF NOT EXISTS campbell (" +\
                    "ID INTEGER PRIMARY KEY," +\
                    "SERIALNO INT NOT NULL" +\
                    ");"
        c.execute(sql)
        self.db.commit()
         
        # now get my ID for this datalogger or create        
        sql = "SELECT ID FROM campbell WHERE SERIALNO = ?"
        c.execute(sql, (self.serno,))
        
        rslt = c.fetchone()
        if not rslt:
            sql = 'INSERT INTO campbell VALUES (Null, ?)'
            c.execute(sql, (self.serno,))
            self.dbid = c.lastrowid
            self.db.commit()
                        
            self.log.debug("Created datalogger in table with id {}".format(self.dbid))
        else:
            self.dbid = rslt[0]
            self.log.debug("Datalogger ID in localdb is {}".format(self.dbid))
            
            
        sql = "CREATE TABLE IF NOT EXISTS datalogger (" +\
                "ID INTEGER PRIMARY KEY," +\
                "campbell_id INTEGER NOT NULL," +\
                "last_upload timestamp," +\
                "last_recno INTEGER);"
        c.execute(sql) 
        self.db.commit()
                
        sql = "SELECT last_upload, last_recno FROM datalogger WHERE campbell_id = ? ORDER BY ID DESC LIMIT 1" 
        c.execute(sql, (self.dbid,))
        
        rslt = c.fetchone()
        if rslt and rslt[0] and rslt[1] and rslt[0] != "Null" and rslt[1] != "Null":            
            self.last_collect["ts"] = rslt[0]
            self.last_collect["NextRecNbr"] = rslt[1]
            self.log.info("Datalogger resuming from recnumber {} last uploaded on {}".format(rslt[1], rslt[0]))
        else:
            self.update_local_db("Null", "Null")
        
        
    def update_local_db(self, timestamp, nextrecnumber):            
        sql = "INSERT INTO datalogger VALUES (Null, ?, ?, ?)"
        self.db.execute(sql, (self.dbid, timestamp, nextrecnumber))
        self.db.commit()
        self.log.debug("Updated localdb with last record {} @ {}".format(nextrecnumber, timestamp))
        
    def stop(self):            
        self.db.commit()
        self.db.close()
        
    def __repr__(self):
        return "Metric ID: " + str(self.metric_id) + "; PakBus ID: " + str(self.pakbus_id) + \
               "; Table List: " + str(self.list_of_tables)

    def getTables(self, socket, count):
        ''' Get all tables in the datalogger except for "Public" and "Status"
            count: maximum number of retries
        '''
        list_of_tables = []
        
        self.log.debug('Getting tables for {}'.format( self.metric_id))
        
        if self.FileData:
            for tableno in range(len(self.tabledef)):
                # ignore tables "Public" and "Status"
                if self.tabledef[tableno]['Header']['TableName'] != "Public" and self.tabledef[tableno]['Header']['TableName'] != "Status":
                    list_of_tables += [Table(self.tabledef[tableno]['Header']['TableName'], self.tabledef, tableno)]
        else:
            if (count > 0):
                self.FileData, RespCode = pakbus.fileupload(socket, self.pakbus_id, self.my_node_id, FileName = '.TDF')                        
                self.log.debug(" -- table def RespCode was: {}".format(RespCode))
                return self.getTables(socket, count-1)
            else:
                # raise exception??
                print "Could not get tables for datalogger with metric: " + str(self.metric_id) + " and pakbus: " + str(self.pakbus_id) + " after 3 tries."
        return list_of_tables
    def emit_all(self, tbl, recs):
        """
        tbl is a table.Table object that represents a Campbell data table. It has a list of tags/ column names/units in the data table.
        Sensor tags are provided by sensorTag.SensorTag 
        Table: 
            Table Name: CR800_2; 
            Table No: 2; 
            Sensor Tags: {'WaterTemp': Sensor Name: WaterTemp; 
                                        Sensor Units: deg_C; 
                                        Sensor Process: Smp, 
                        'WaterLevel': Sensor Name: WaterLevel; 
                                    Sensor Units: meters; 
                                    Sensor Process: Smp, 
                        'BattV': Sensor Name: BattV; 
                                Sensor Units: Volts; 
                                Sensor Process: Smp, 
                        'PTemp_C': Sensor Name: PTemp_C; 
                                Sensor Units: Deg_C; 
                                Sensor Process: Smp}
        
        recs is the list of data records. 
        <type 'list'>: [
                        {'Fields': {'WaterTemp': [15.49], 'WaterLevel': [0.052], 'BattV': [13.85], 'PTemp_C': [20.02]},
                         'RecNbr': 70558, 'TimeOfRec': (813241500, 0)}, 
                         
                         {'Fields': {'WaterTemp': [15.53], 'WaterLevel': [0.052], 'BattV': [13.84], 'PTemp_C': [20.26]}, 
                         'RecNbr': 70559, 'TimeOfRec': (813241800, 0)}, 
                         
                         {'Fields': {'WaterTemp': [15.57], 'WaterLevel': [0.052], 'BattV': [13.8], 'PTemp_C': [20.48]}, 
                         'RecNbr': 70560, 'TimeOfRec': (813242100, 0)}, 
                         
                         {'Fields': {'WaterTemp': [15.6], 'WaterLevel': [0.052], 'BattV': [13.8], 'PTemp_C': [20.72]}, 
                         'RecNbr': 70561, 'TimeOfRec': (813242400, 0)}, 
                         
                         {'Fields': {'WaterTemp': [15.64], 'WaterLevel': [0.052], 'BattV': [13.81], 'PTemp_C': [20.96]}, 
                         'RecNbr': 70562, 'TimeOfRec': (813242700, 0)}, 
                         
                         {'Fields': {'WaterTemp': [15.68], 'WaterLevel': [0.052], 'BattV': [13.8], 'PTemp_C': [21.15]},                          
                         'RecNbr': 70563, 'TimeOfRec': (813243000, 0)}, 
                         
                         {'Fields': {'WaterTemp': [15.73], 'WaterLevel': [0.052], 'BattV': [13.82], 'PTemp_C': [21.31]}, 
                         'RecNbr': 70564, 'TimeOfRec': (813...
        
        """


        #self.log.info("emit: {}, recs: {}".format(tbl, recs))
        
        if self.data_callback:            
            for rec in recs:
                timestamp = rec['TimeOfRec'][0] + 631166400                
                # emit record number
                self.data_callback (timestamp, rec['RecNbr'], tbl.record_tag)
                # and all fields                        
         
                #if self.wu_object and tbl.name == self.wu_object.table:
                    #self.wu_object.put(timestamp, rec['Fields'])
                
                
                if self.record_callback:
                    self.record_callback(tbl.name, timestamp, rec)
                
                for sensor_name, values in rec['Fields'].iteritems():                    
                    self.data_callback(timestamp,                                        
                                       values[0],
                                       tbl.dic_of_sensorTags[sensor_name].tag)
        
        
                
        
    def collect_all(self, socket):                        
        for tbl in self.list_of_tables:
        
            self.log.debug("Collecting all from {}.{}".format(self.metric_id, tbl.name))

            more_flag = True            
            # collect most recent record
            data, more_flag = pakbus.collect_data(socket, self.pakbus_id,
                                                  self.my_node_id, self.tabledef,
                                                  tbl.name,
                                                  CollectMode = 0x03)
            
            if data and len(data) > 0:
            
                self.last_collect['ts'] = datetime.datetime.now()
                # compute next record to collect
                self.last_collect['NextRecNbr'] = data[0]['BegRecNbr'] + data[0]['NbrOfRecs'] 
                
                self.emit_all(tbl, data[0]['RecFrag'])
                
                self.update_local_db(self.last_collect['ts'], self.last_collect['NextRecNbr'])         
                #if self.data_callback:              
                    #for rec in data[0]['RecFrag']:
                        #self.data_callback(self, rec)
            
            if more_flag:
                self.collect_increment(socket)
            
    
    def collect_increment(self, socket):
        for tbl in self.list_of_tables:
        
            self.log.debug("Collecting increment from {}.{} NextRecNbr = {}".format(self.metric_id, 
                                                                                    tbl.name,
                                                                                    self.last_collect['NextRecNbr']))
            more_flag = True
            while more_flag:
                
                # collect from where we left off
                data, more_flag = pakbus.collect_data(socket, self.pakbus_id,
                                                      self.my_node_id, self.tabledef,
                                                      tbl.name,
                                                      CollectMode = 0x04,
                                                      P1 = self.last_collect['NextRecNbr'])   
        
                if data and len(data) > 0:        
                    self.last_collect['ts'] = datetime.datetime.now()
                    # compute next record to collect
                    self.last_collect['NextRecNbr'] = data[0]['BegRecNbr'] + data[0]['NbrOfRecs'] 
                    
                    # possible loss of data, we should wait for emit all to complete before
                    # committing this... but forget it for now.
                    self.update_local_db(self.last_collect['ts'], self.last_collect['NextRecNbr'])
                    
                    self.emit_all(tbl, data[0]['RecFrag'])
                    
                    if more_flag:
                        self.log.info("{}.{} has more records to fetch".format(self.metric_id, tbl.name))
                    else:
                        self.log.info("{}.{} is current".format(self.metric_id, tbl.name))
                    #if self.data_callback:              
                        #for rec in data[0]['RecFrag']:
                            #self.data_callback(tbl.dic_of_sensorTags, rec)                
                else:
                    self.log.warn("Collecting data but got no records.")
    def collect(self, socket):
        
        if self.last_collect['ts'] == None:
            # initial collection, get all data
            self.collect_all(socket)
        else:
            # incremental collect
            self.collect_increment(socket)
            

            
            
    def collectData(self, addition_size, num_dataloggers):
        ''' Collect data from datalogger up to "addition_size" number of records '''

        raise Exception ("depreciated") 

        up_to_date = False
        datalogger_size = len(self.list_of_tables)
        start_record_numbers = [0]*datalogger_size
        all_start_records = [0]*num_dataloggers
        try:
            if os.stat("lastRecord.txt").st_size > 0:
                with open("lastRecord.txt", "r") as f:
                    all_start_records = f.read().split('\n')
                    start_record_numbers = all_start_records[self.number].split(',')
            else:
                # empty file
                start_record_numbers = [0]*datalogger_size
        except OSError:
                # no file
                start_record_numbers = [0]*datalogger_size

        end_record_numbers = [int(x) for x in start_record_numbers]

        #print 'datalogger_size: ', datalogger_size



        for i in range(datalogger_size):
            data = pakbus.collect_data(self.socket, self.pakbus_id, 
                                       self.my_node_id, self.tabledef, 
                                       self.list_of_tables[i].name, 
                                       CollectMode = 0x05, P1 = 1)

            start_time = time.time()

            if (len(data[0]) == 0):
                # rarely happends
                # dont know the cause
                 #print 'got here for some reason'
                 break

            most_recent_record_number = data[0][0]['RecFrag'][0]['RecNbr']

            
            data_to_add_to_database = []
            current_table = self.list_of_tables[i]
            record_number = int(start_record_numbers[i])
            for j in range(addition_size):
                data = pakbus.collect_data(self.socket, self.pakbus_id, self.my_node_id, self.tabledef, self.list_of_tables[i].name, \
                                           CollectMode = 0x06, P1 = record_number, P2 = record_number+1)


                if (len(data[0]) == 0):
                    # gets here because msg is an empty dictionary
                    # only happens for CR800, as if fails connections sometimes
                    # never happens for CR1000
                    # line 1527 in pakbus.py
                    # got here because there is no data
                    j -= 1 # dont add so dont count
                    #print 'got here instead'
                    break

                time_stamp = data[0][0]['RecFrag'][0]['TimeOfRec'][0] + 631166400
                sensors = data[0][0]['RecFrag'][0]['Fields']

                #print data


                for sensor in sensors.keys():
                    # to avoid problem with "NaN"
                    try:
                        long(sensors[sensor][0])
                        data_to_add_to_database += [(time_stamp, sensors[sensor][0], 
                                                     self.list_of_tables[i].dic_of_sensorTags[sensor].tag)]
                    except:
						print 'a data point was NaN or invalid'
                        # exclude data point
                        # data_to_add_to_database += [(time_stamp, -1, self.list_of_tables[i].dic_of_sensorTags[sensor].tag)]

                data_to_add_to_database += [(time_stamp, record_number, self.list_of_tables[i].record_tag)]

                record_number += 1

                if (record_number > most_recent_record_number):
                    up_to_date = True
                    #print 'got here'
                    break
                else:
                    up_to_date = False
            #print len(data_to_add_to_database)
            #print time.time()-start_time
            #start_time = time.time()
            #print data_to_add_to_database
            #return
            if (len(data_to_add_to_database) > 0):
                print client.multiplePut(data_to_add_to_database) + ' records added: ' + str(j+1)
                #print time.time()-start_time
            end_record_numbers[i] = record_number

        
        all_start_records[self.number] = str(end_record_numbers)[1:-1]

        all_end_records = ""
        for records in all_start_records:
            all_end_records = all_end_records + str(records) + '\n'
        all_end_records = all_end_records[0:-1]

        

        with open("lastRecord.txt", "w") as f:
                f.write(all_end_records)
        
        return up_to_date

