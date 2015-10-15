import requests
import logging
import datetime
import pytz

class pws_upload():
    def __init__(self,
                 pwsid,
                 password, 
                 url = "http://weatherstation.wunderground.com/weatherstation/updateweatherstation.php"):
        
        self.pwsid = pwsid
        self.password = password
        self.url = url
        self.log = logging.getLogger(__name__)
        
    def put(self, **kwargs):
        "all kwargs get passed into wunderground"
        
        params = dict(kwargs)
        params['ID'] = self.pwsid
        params['PASSWORD'] = self.password
        r = requests.get(self.url, params = params)
        
        if r.status_code != 200:
            self.log.warn("PWS upload failed with: {}".format(r.text))
        
        return r.status_code, r.text
                
class campbell_adapter():
    
    cs_to_wu = {'WindDir': 'winddir',
                'WindSpeed': 'windspeedmph', # convert
                'Rain_Tot': 'rainin',  # to last hour
                'AirTemp': 'tempf',  # convert c to f  
                'RelHum': 'humidity',
                'Solar_kW': 'solarradiation',
                }
    
    def __init__(self, uploader, table):
        """
        uploader is the pws upload object to use
        table is the weather station's table name, we will try to push all args
        
        """
        self.table = table
    def convert_time(self, ts):
        if isinstance(ts, datetime.date):
            ts = datetime.datetime(ts.year, ts.month, ts.day)
        return int((ts - datetime.datetime(1970,1,1) +
                datetime.timedelta(hours=4)).total_seconds())

    def unconvert_time(self, uxtime):
        return datetime.datetime(1970,1,1,tzinfo=pytz.timezone('US/Eastern')) + \
            datetime.timedelta(seconds=uxtime) - \
            datetime.timedelta(hours=4)
        
    def put(self, timestamp, record):
        """
        map the campbell data entry to a wunderground put
        
        timestamp is the UNIX timestamp
        """ 
        ts = self.unconvert_time(timestamp)
        
        data = {'dateutc': ts.strftime('%Y-%m-%d %H:%M:%S')}
        
        self.uploader.put(*data)            

if __name__ == "__main__":
    
    logging.basicConfig(level=logging.DEBUG)
    wu = pws_upload('KPALEWIS11',
                    'FqqGy5GZ')
    
    wu.put(softwaretype="python", tempf = 80, dateutc = "2014-01-01 12:00:00")
    