import logging

from wunder import pws_upload

from examples.mongolayer import mongodb
import pymongo
import datetime
import pytz
import math
from pint import UnitRegistry
from test.test_support import temp_cwd

ureg = UnitRegistry()

__version__ = '0.1'
def ts_to_ux(self, ts):        
    return int ( (ts-datetime.datetime(1970,1,1,tzinfo=pytz.timezone('UTC'))).total_seconds() )
    
def ux_to_utc(uxtime):
    return datetime.datetime(1970,1,1,tzinfo=pytz.timezone('UTC')) + \
        datetime.timedelta(seconds=uxtime)
        
def c_to_f(c):
    return ureg.Quantity(c, ureg.degC).to(ureg.degF).magnitude
def c_to_k(c):
    return ureg.Quantity(c, ureg.degC).to(ureg.degK).magnitude 

def ms_to_mph(ms):
    "meters per second (ms) to miles per hour"
    return (ureg.Quantity(ms, ureg.meter).to(ureg.mile) * 3600).magnitude
def kw_to_w(kw):
    return kw * 1000

def sat_press(temp_c):
    """saturated vapor pressure 
    
    ref: http://www.srh.noaa.gov/images/epz/wxcalc/vaporPressure.pdf
    """
    f = (7.5 * temp_c) / (237.3 + temp_c)
    return 6.11 * 10 ** (f)

def dewpoint(temp_c, rel_hum):
    """rel_hum = 90.5 means 90.5% (don't use 0.905). Result is in C
    
    ref: http://www.srh.noaa.gov/images/epz/wxcalc/wetBulbTdFromRh.pdf
    """

    e_s = sat_press(temp_c)
    
    return (237.3 * math.log(e_s*rel_hum/611))/ \
            (7.5 * math.log(10) - math.log(e_s*rel_hum/611))
    
if 0:
    "computes a dew point chart"
    print "   ",
    for temp_c in range(20,120, 10):
        print "{:6d} ".format(temp_c),
    print
    for hum in range(20,100,10):
        print "{:2.0f} ".format(hum),
        for temp_c in range(20,120, 10):
            print "{:6.2f} ".format(dewpoint(temp_c, hum)),
        print

if __name__ == "__main__":
        
    logging.basicConfig(level=logging.INFO)
    wu = pws_upload('KPALEWIS11',
                    'FqqGy5GZ')
    
    db = mongodb('mongodb://amm-csr1/',
                 dbname = 'raw_data',
                 collection = 'miller_run')

    i = 0
    for doc in db.get_oldest_unpushed(start = datetime.datetime.utcnow().replace(tzinfo=pytz.utc) - datetime.timedelta(days=10)): 
                                      #wunderground_push = {"$exists": True}): 
        sample_time = ux_to_utc(doc['Timestamp'])
        logging.info("pushing time: {}".format(sample_time))
  
        if datetime.datetime.utcnow().replace(tzinfo=pytz.utc) - sample_time < datetime.timedelta(days=5):
        
            wind_2m = db.get_avg(('WindSpeed', 'WindDir'),
                                 sample_time - datetime.timedelta(minutes=2), 
                                 sample_time)    
      
            result, text = wu.put(
                   softwaretype="wunderground_push" + __version__,
                   dateutc = sample_time.strftime('%Y-%m-%d %H:%M:%S'),
                   
                   winddir = doc['WindDir'],
                   windspeedmph = ms_to_mph(doc['WindSpeed']),
                   
                   windspdmph_avg2m = ms_to_mph(wind_2m['WindSpeed']),
                   winddir_avg2m = wind_2m['WindDir'],
                   
                   humidity = doc['RelHum'],
                   dewptf = c_to_f ( dewpoint ( doc['AirTemp'], doc['RelHum'])),
                   tempf = c_to_f(doc['AirTemp']),
                   
                   rainin = db.get_sum( ('Rain_Tot', ), 
                                        sample_time - datetime.timedelta(seconds=60*60),
                                        sample_time)['Rain_Tot'],
                   
                   dailyrain = db.get_sum( ('Rain_Tot', ), 
                                        datetime.datetime(sample_time.year,
                                                          sample_time.month,
                                                          sample_time.day,
                                                          tzinfo= pytz.timezone('UTC')),
                                        sample_time)['Rain_Tot'],
                   solarradiation = kw_to_w(doc['Solar_kW'])
                   
                   )
        else:
            result = 200
            text = "skipped, more than 1 week old"
            
        
        if result == 200:
            #doc.update({'wunderground_push': datetime.datetime.utcnow() })
            rslt = db.collection.update({'_id': doc['_id']}, 
                                        {'$set': {'wunderground_push': datetime.datetime.utcnow() }})
            logging.info("Success: {}, marked push".format(text.strip()))
        else:
            logging.warn("Error pushing to wunderground: {}".format(text))

        
    #cursor = db.get({'wunderground_push': {"$exists": True}})        
    #for doc in cursor: 
        #logging.debug("pushed doc: {}".format(doc))
        #db.collection.update({'_id': doc['_id']},
         #                    {'$unset': {"wunderground_push": ""}})
    logging.warn("Done")
    
