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
                 security_code = 0x1111):
        #self.number = number

        self.log = logging.getLogger(__name__)
        
        self.db = sqlite3.connect(db)

        self.my_node_id = my_node_id
        self.metric_id = metric_id
        self.pakbus_id = pakbus_id
        self.security_code = security_code
        self.data_callback = data_callback
        
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
        #self.data_callback(tbl.dic_of_sensorTags, rec)
        
                        #if self.data_callback:              
                    #for rec in data[0]['RecFrag']:
                        #self.data_callback(tbl.dic_of_sensorTags, rec)  
            
        if self.data_callback:            
            for rec in recs:
                timestamp = rec['TimeOfRec'][0] + 631166400                
                # emit record number
                self.data_callback (timestamp, rec['RecNbr'], tbl.record_tag)
                # and all fields
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

