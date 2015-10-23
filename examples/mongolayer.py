
import pymongo

import logging
import datetime
import pytz
import numpy

__version__ = '0.1.0'

class mongodb( ):
    
    def __init__(self, 
                 server,
                 dbname = 'data',
                 collection = 'generic_collection'):
        
        self.log = logging.getLogger(__name__)
        self.server = server
        self.dbname = dbname
        self.collectionname = collection
        
        self.client = pymongo.MongoClient(server)
        self.db = self.client[self.dbname]
        self.collection = self.db[self.collectionname]
        
        self.collection.ensure_index('Timestamp')
        
        self.log.info("Using {} - {}".format(self.client,
                                             self.collection))
  
    def ts_to_ux(self, ts):
        
        return int ( (ts-datetime.datetime(1970,1,1,tzinfo=pytz.timezone('UTC'))).total_seconds() )
    
    def ux_to_utc(self, uxtime):
        return datetime.datetime(1970,1,1,tzinfo=pytz.timezone('UTC')) + \
            datetime.timedelta(seconds=uxtime)    
        
    def add_record(self, tablename, timestamp, record):
        """
        datalogger tables need clean up
        {'Fields': 
            {'AirTemp': [5.078], 
            'Rain_Tot': [0.0], 
            'RelHum': [100.0], 
            'WaterLevel': [1.493], 
            'WindDir': [151.4], 
            'WindSpeed': [0.0], 
            'Solar_kW': [0.0], 
            'BattV': [12.67], 
            'WaterTemp': [15.39], 
            'Solar_MJ_Tot': [0.0], 
            'PTemp_C': [4.90] ....
            
        """
        
        
        r= {key: val[0] for key,val in record['Fields'].items()}
            
        r['Timestamp'] = timestamp
        r['TableName'] = tablename        
        r['DownloadTimeUTC'] = datetime.datetime.utcnow()
        r['DownloadVersion'] = __version__
        
        self.log.debug("insert - {}".format(r))
        self.collection.insert(r)
        
    
    def get_oldest_unpushed(self, 
                            start = None,
                            end = None, 
                            wunderground_push = {"$exists": False },
                            timeout = True):
    
        if start == None and end == None:
            tsq = {"$exists": True}            
        else:           
            tsq = {}
            if start:
                tsq["$gte"] = self.ts_to_ux(start)
            if end:
                tsq["$lt"] = self.ts_to_ux(end)
        
        return self.collection.find(
                    { 'wunderground_push': wunderground_push,
                      'Timestamp': tsq,
                      'TableName': 'Table'},
                    no_cursor_timeout = not timeout
                    ).sort('Timestamp', pymongo.ASCENDING)
                    
    def get_agg(self, params, start, end, agg):
        """
        return the aggregation of each item in params over the time
        from start to end (python datetime objects)
        
        """

        docs = self.collection.find(
                    {'TableName': 'Table', 
                     'Timestamp': {'$gte': self.ts_to_ux(start),
                                   '$lte': self.ts_to_ux(end)}
                     })
        
        result = {key:[] for key in params}
        for doc in docs:                    
            for param in params:
                if param in doc:                       
                    result[param] += [doc[param]]
    
        return {key: agg(val) for key, val in result.items()}            
         
    def get_sum(self, params, start, end):
        """
        return the sum of each item in params over the time
        from start to end (python datetime objects)
        
        """
        return self.get_agg(params, start, end, agg=numpy.sum)
    def get_avg(self, params, start, end):
        """
        return the average of each item in params over the time
        from start to end (python datetime objects)
        
        """
        return self.get_agg(params, start, end, agg=numpy.mean)
        
        #for doc in docs:
            #print (self.ux_to_utc(doc['Timestamp']), doc[params[1]])
        #return { key: numpy.mean(filter (docs, ))}

if __name__ == "__main__":
    db = mongodb('mongodb://amm-csr1/',
                 dbname = 'raw_data',
                 collection = 'miller_run')
    sample_time = datetime.datetime(2015, 9, 1, tzinfo=pytz.timezone('UTC'))
    r = db.get_avg((u'WindSpeed', u'WindDir'),
                sample_time - datetime.timedelta(minutes=60), 
                sample_time)
    print(r)
        
        
        
