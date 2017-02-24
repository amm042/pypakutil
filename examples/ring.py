# this runs with python 2.7 - ish.

# code to ring a node using link-level messaging

# setup logging FIRST.
import logging
LOGFMT = '%(asctime)s %(name)-30s %(levelname)-8s %(message).240s'
logging.basicConfig(level = logging.WARN,
                    format = LOGFMT)

import optparse
import ConfigParser, StringIO
from datalogger import DataLogger
#import logging
import pakbus
import time
import json
import socket
import paksock
import requests

#from API import SensorClient

import threading
from threading import Event

from Queue import Queue

from sensorTag import SensorTag

from mongolayer import mongodb

#from wunder import pws_upload, campbell_adapter

class DatabaseAL (threading.Thread):
    "database abstraction layer"

    def __init__(self, server, enabled = 0):
        threading.Thread.__init__(self)
        self.name = "DatabaseAL worker thread"
        self.setDaemon(True)
        self.log = logging.getLogger('DatabaseAL')
        #self.client = SensorClient()
        self.server = server

        self.enabled = enabled
        if self.enabled == 0:
            self.log.warning("OpentTSDB is disabled. Data will not be saved in OpenTSDB.")

        self.dataq = Queue(maxsize = 1024*1024)
        self.shutdown_evt = Event()

    def append(self, timestamp, values, tags):
        "append data to the db"
        #self.log.debug("Got data: {} {} {}".format(timestamp, values, tags))
        # append a tuple
        #if type(tags) is SensorTag:
        #    print tags

        if self.dataq.full():
            self.log.error("DataQ is full! Data is lost!")
        else:
            self.dataq.put( (timestamp, values, tags) )

    def busy(self):
        return not self.dataq.empty()

    def put(self, worklist):
        # format is (unix timestamp, value, Tags)

        m = set()
        t = set()
        p = []
        for w in worklist:
            ts, val, tags = w
            if tags.metric not in m:
                m.add(tags.metric)
            for k,v  in tags.toTagData().items():
                kv_str = "{}={}".format(k,v)
                if kv_str not in t:
                    t.add(kv_str)

            p += [{'metric': tags.metric,
                      'timestamp': ts,
                      'value': val,
                      'tags': tags.toTagData(),
                      }]
        #self.log.debug("putting data: "+json.dumps(p))

        self.log.info("putting metrics:"+str(m))
        self.log.info("with tags:"+str(t))
        r = requests.post('{}/put?details'.format(self.server),
                                                  data = json.dumps(p))
        if r.status_code != requests.codes.ok:
            self.log.error("put failed: "+ r.text)

            for ts, val, tags in worklist:
                self.append( ts, val, tags)
        #else:
            #self.log.info("put {} samples.".format(len(worklist)))

    def run(self):
        while not self.shutdown_evt.wait(1):
            if self.shutdown_evt.is_set():
                break
            if self.dataq.empty():
                continue

            worklist = []
            while not self.dataq.empty() and len(worklist) < 2500:
                worklist.append(self.dataq.get())

            self.log.info("Putting a batch of {} samples".format(len(worklist)))
            # now batch processes the worklist

            if len(worklist) > 0:
                try:
                    #i = self.client.multiplePut(worklist)

                    if self.enabled > 0:
                        self.put(worklist)
                        self.log.info("Pushed {} samples to OpenTSDB.".format(len(worklist)))

                except Exception as x:
                    self.log.error("Push failed, error ={}; worklist = {}".format(x, worklist))

        self.log.info("Shutdown")

    def stop(self):
        self.shutdown_evt.set()
        self.join(5)

def main():

    # Start logging
    #logging.basicConfig(filename="PyPakLogging.log", level=logging.DEBUG)
    logging.basicConfig(level=logging.DEBUG) # start at debug level.
    logging.info("Reading config file...")

    # Parse command line arguments
    parser = optparse.OptionParser()
    parser.add_option('-c', '--config',
                      help = 'read configuration from FILE [default: %default]',
                      metavar = 'FILE',
                      default = 'campbell.conf')
    (options, args) = parser.parse_args()

    # Read configuration file
    cf = ConfigParser.SafeConfigParser()
    logging.info('configuration read from {}'.format(cf.read(options.config)))

    if cf.has_option('logging', 'level'):
        logging.info("info config has logging level: {}".format(cf.get('logging', 'level')))
        logging.getLogger().setLevel(logging.getLevelName(cf.get('logging','level')))

    if cf.has_option('pakbus', 'logfile'):
        fh = logging.FileHandler(cf.get('pakbus', 'logfile'))
        fmt = logging.Formatter(LOGFMT)
        fh.setFormatter(fmt)
        logging.getLogger('').addHandler(fh)

    # get host and port
    host = cf.get('pakbus', 'host')
    port = cf.get('pakbus', 'port')

    # get timeout
    timeout = cf.getint('pakbus', 'timeout')

    # get my_node_id
    my_node_id = int(cf.get('pakbus', 'my_node_id'), base = 0)

    # get addition_size
    addition_size = int(cf.get('pakbus', 'addition_size'), base = 0)


    # get time frequency to update database
    database_update_time_gap = int(cf.get('pakbus', 'database_update_time_gap'), base = 0)

    # get time frequency to update database connections
    connection_retry_interval = int(cf.get('pakbus', 'connection_retry_interval'), base = 0)

    localdb = cf.get('pakbus', 'localdb')

    dataloggers = json.loads(cf.get('pakbus', 'datalogger_pakbus_id'))
    sec_codes = json.loads(cf.get('pakbus', 'datalogger_security_code'))

    # get pakbus_id and metric_id information for dataloggers
    #pakbus_ids = cf.get('pakbus', 'pakbus_ids').split(',')
    #print "pakbus_ids: " + str(pakbus_ids)
    #metric_ids = cf.get('pakbus', 'metric_ids').split(',')
    #print "metric_ids: " + str(metric_ids)

    #exit()

    #wu = None
    #if cf.getint('wunderground', 'enabled') > 0:
    #    pws = pws_upload(pwsid = cf.get('wunderground', 'PWSID'),
    #                     password = cf.get('wunderground', 'password'),
    #                     url = cf.get('wunderground', 'url'))
    #
    #    wu = campbell_adapter(pws, cf.get('wunderground', 'table'))

    logging.debug('Using dataloggers = {}'.format( dataloggers))

    logging.getLogger("SensorAPI_API").setLevel(logging.INFO)

    logging.getLogger("ZeroMQLayer.ZeroMQClient").setLevel(logging.DEBUG)
    try:
        db = None
        db = DatabaseAL(cf.get("opentsdb", "server"),
                        cf.getint("opentsdb", "enabled"))
        db.start()

        recordadd = None
        if cf.getint('mongodb', 'enabled') > 0:
            mongo = mongodb(cf.get("mongodb", "server"),
                            cf.get("mongodb", "db"),
                            cf.get("mongodb", "collection")
                            )
            recordadd = mongo.add_record


        while True:
            try:

                logging.info('MAKING CONNECTION')

                # open socket
                skt = None
                while skt == None:
                    #skt = pakbus.open_socket(host, port, timeout)
                    skt = paksock.PakSock(host, port, timeout, my_node_id)
                    if skt == None:
                        logging.error("Failed to open socket, retry")
                    logging.info(" ... waiting for connect")
                    skt.have_socket_evt.wait()


                #if (len(pakbus_ids) != len(metric_ids)):
                    #logging.error("'pakbus_ids' and 'metric_ids' need to have the same number of elements")

                #logging.info("Finished reading config file. Now making datalogger instances...")

                # create list datalogger instances
                logging.info( 'MAKING DATALOGGERS')

                #datalogger_list = []
                #for i in range(len(pakbus_ids)):
                #    print metric_ids[i]
                #    tmp = [DataLogger(i, socket, my_node_id, metric_ids[i], int(pakbus_ids[i], base = 0))]
                #    print len(tmp)
                #    datalogger_list += [DataLogger(i, socket, my_node_id, metric_ids[i], int(pakbus_ids[i], base = 0))]

                #replace the pakbus address with an instance of datalogger
                for metric, address in dataloggers.iteritems():
                    dataloggers[metric] = DataLogger(localdb,
                                                     skt,
                                                     my_node_id,
                                                     metric,
                                                     address,
                                                     db.append,
                                                     recordadd,
                                                     sec_codes[metric],
                                                     )

                #logging.info("Finished making datalogger instances. Now collecting data...")

                logging.info( 'COLLECTING DATA')

                while True:

                    for metric, dl in dataloggers.iteritems():
                        dl.collect(skt)

                    logging.info("got samples all, waiting for opentsdb queue to empty")

                    while (db.busy()):
                        time.sleep(1)

                    logging.info("all done and pushed, exiting")
                    exit();

                    #shutdown connection
                    #skt.shutdown(socket.SHUT_RDWR)
                    #skt.close()
                    #skt = None
                    skt.shutdown()
                    time.sleep(database_update_time_gap)

                    # reopen socket
                    while not skt.have_socket:
                        skt.open()
                        if not skt.have_socket:
                            logging.error("Failed to reopen socket, retry")
                            time.sleep(5)

            except socket.error as msg:
                logging.error("Socket died with: {}".format(msg))
                if skt:
                    skt.close()
                skt = None

                if 0:

                    up_to_date = False;
                    count = 0
                    while True:
                        if up_to_date:
                            print "UP TO DATE; now waiting"
                            time.sleep(database_update_time_gap)
                        try:
                            if (count == connection_retry_interval):
                                datalogger_list = []
                                for i in range(len(pakbus_ids)):
                ##                    print metric_ids[i]
                                    datalogger_list += [DataLogger(i, socket, my_node_id, metric_ids[i], int(pakbus_ids[i], base = 0))]
                                count = 0
                            up_to_date = True
                            for i in range(len(datalogger_list)):
                                print datalogger_list[i].metric_id
                                up_to_date = datalogger_list[i].collectData(addition_size, len(datalogger_list)) and up_to_date
                        except:
                            continue
                        count += 1
    finally:
        if db:
            db.stop()

# want addition_size to be greater than the number of records taken during database_update_time_gap



if __name__ == "__main__":

    main()
