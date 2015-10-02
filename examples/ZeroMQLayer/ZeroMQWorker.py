from random import randint
import time
import zmq
import requests
import json
import logging
import ConfigParser
import os
import sys
import MySQLdb


class _WorkerConf(object):
    def __init__(self):
        self.__conf = ConfigParser.ConfigParser()
        hasFile = False
        if os.path.isfile("ZeroMQWorker.conf"):
            self.__conf.read("ZeroMQWorker.conf")
            logging.info("Using configuration file at ZeroMQWorker.conf")
            hasFile = True
        else:
            logging.warn("Could not find ZeroMQWorker.conf, Use default setting now")

        self.queueIP = self.__conf.get("ZeroMQQueueServer", "ServerIP") if hasFile else "localhost"
        self.queuePort = self.__conf.getint("ZeroMQQueueServer", "ServerPort") if hasFile else 5556
        self.workerLiveness = self.__conf.getfloat("Worker", "HeartBeatLiveness") if hasFile else 3.0
        self.heartbeatInterval = self.__conf.getfloat("Worker", "HeartBeatInterval") if hasFile else 1.0
        self.workerIntervalInit = self.__conf.getint("Worker", "IntervalInit") if hasFile else 1
        self.workerIntervalMax = self.__conf.getint("Worker", "IntervalMax") if hasFile else 32
        self.openTSDBIP = self.__conf.get("OpenTSDB", "ServerIP") if hasFile else "localhost"
        self.openTSDBPort = self.__conf.get("OpenTSDB", "ServerPort") if hasFile else 4242

    def getQueueIP(self):
        return self.queueIP

    def getQueuePort(self):
        return self.queuePort

    def getWorkerHeartBeatLiveness(self):
        return self.workerLiveness

    def getHeartBeatInterval(self):
        return self.heartbeatInterval

    def getWorkerIntervalInit(self):
        return self.workerIntervalInit

    def getWorkerIntervalMax(self):
        return self.workerIntervalMax

    def getOpenTSDBIP(self):
        return self.openTSDBIP

    def getOpenTSDBPort(self):
        return self.openTSDBPort

class ZeroMQWorker(object):
    def __init__(self):
        self.__conf = _WorkerConf()
        self.HEARTBEAT_LIVENESS = self.__conf.getWorkerHeartBeatLiveness()
        self.HEARTBEAT_INTERVAL = self.__conf.getHeartBeatInterval()
        self.INTERVAL_INIT = self.__conf.getWorkerIntervalInit()
        self.INTERVAL_MAX = self.__conf.getWorkerIntervalMax()

        self.QUEUE_SERVERIP = self.__conf.getQueueIP()
        self.QUEUE_SERVERPORT = self.__conf.getQueuePort()
        self.OPENTSDB_SERVERIP = self.__conf.getOpenTSDBIP()
        self.OPENTSDB_PORT = self.__conf.getOpenTSDBPort()

        #  Paranoid Pirate Protocol constants
        self.PPP_READY = "\x01"      # Signals worker is ready
        self.PPP_HEARTBEAT = "\x02"  # Signals worker heartbeat


        self.worker = None
        self.context = None
        self.poller = None

        self.s = requests.Session()
        # Get connection session
        self.s.get("http://{0}:{1}".format(self.OPENTSDB_SERVERIP, self.OPENTSDB_PORT))
        pass

    def connect(self):
        self.context = zmq.Context(1)
        self.worker = self.context.socket(zmq.DEALER)   # Dealer
        self.poller = zmq.Poller()

        identity = "%04X-%04X" % (randint(0, 0x10000), randint(0, 0x10000))
        self.worker.setsockopt(zmq.IDENTITY, identity)
        self.poller.register(self.worker, zmq.POLLIN)
        self.worker.connect("tcp://{0}:{1}".format(self.QUEUE_SERVERIP, self.QUEUE_SERVERPORT))
        self.worker.send(self.PPP_READY)


    def run(self):
        liveness = self.HEARTBEAT_LIVENESS
        interval = self.INTERVAL_INIT

        heartbeat_at = time.time() + self.HEARTBEAT_INTERVAL
        cycles = 0

        while True:
            socks = dict(self.poller.poll(self.HEARTBEAT_INTERVAL * 1000))

            # Handle worker activity on backend
            if socks.get(self.worker) == zmq.POLLIN:
                #  Get message
                #  - 3-part envelope + content -> request
                #  - 1-part HEARTBEAT -> heartbeat
                frames = self.worker.recv_multipart()
                if not frames:
                    break # Interrupted

                if len(frames) == 3:
                    logging.info("Normal reply")
                    input = frames[2]
                    data = json.loads(input)
                    if "db" in data:
                        data.pop("db", None)
                        frames[2] = self.getData(data)
                    else:
                        frames[2] = self.__postRequests(input).encode('ascii', 'ignore')

                    self.worker.send_multipart(frames)
                    liveness = self.HEARTBEAT_LIVENESS


                elif len(frames) == 1 and frames[0] == self.PPP_HEARTBEAT:
                    logging.info("Queue heartbeat")
                    liveness = self.HEARTBEAT_LIVENESS
                else:
                    logging.error("E: Invalid message: %s" % frames)
                interval = self.INTERVAL_INIT
            else:
                liveness -= 1
                if liveness == 0:
                    logging.warn("W: Heartbeat failure, can't reach queue")
                    logging.warn("W: Reconnecting in %0.2f..." % interval)
                    time.sleep(interval)

                    if interval < self.INTERVAL_MAX:
                        interval *= 2
                    else:
                        logging.error("Could not connect to queue. Program quits")
                        sys.exit()
                    self.poller.unregister(self.worker)
                    self.worker.setsockopt(zmq.LINGER, 0)
                    self.worker.close()

                    self.connect()  # reconnecto

                    liveness = self.HEARTBEAT_LIVENESS
            if time.time() > heartbeat_at:
                heartbeat_at = time.time() + self.HEARTBEAT_INTERVAL
                #print "I: Worker heartbeat"
                self.worker.send(self.PPP_HEARTBEAT)

    def __postRequests(self, jsData):
        headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
        data = json.loads(jsData)
        method = data["method"]
        requestData = data["data"]
        url = "http://{0}:{1}/api/{2}".format(self.OPENTSDB_SERVERIP, self.OPENTSDB_PORT, method)
        r = self.s.post(url, data=json.dumps(requestData), headers=headers)
        return r.text



    def getData(self, inputForm):
        start = inputForm["start"]
        end = inputForm["end"]
        metrics = inputForm["queries"]["metrics"]
        dbType = inputForm["queries"]["dbType"]

        '''
        if dbType == "KeyiDB":
            aggregator = jsonForm[4]["extra"]["aggregator"]
            downsample = jsonForm[4]["extra"]["downsample"]
            rate = jsonForm[4]["extra"]["rate"]
            tags = jsonForm[4]["extra"]["tags"]
        '''

        if dbType == "MySQL":
            table = inputForm["queries"]["table"]

            # Open database connection
            db = MySQLdb.connect("db.eg.bucknell.edu","sri","Hee1quai")

            # prepare a cursor object using cursor() method
            cursor = db.cursor()

            # use metrics to get site and parameter info
            siteIDs = []
            paramIDs = []
            for metric in metrics:
                siteID_query = "SELECT id FROM sri.sample_site WHERE name='"+metric.split(".")[0]+"'"
                cursor.execute(siteID_query)
                siteIDs += [cursor.fetchone()[0]]

                paramID_query = "SELECT id FROM sri.sample_parameter WHERE name='"+metric.split(".")[1]+"'"
                cursor.execute(paramID_query)
                paramIDs += [cursor.fetchone()[0]]


            # Command to query data
            temp = "s"+str(siteIDs[0]) + "p"+str(paramIDs[0])
            # use for every sample value
    ##        sql = "SELECT "+temp+".datetime as date, "
            # use for daily average value
            sql = "SELECT DATE(FROM_UNIXTIME("+temp+".datetime)) as date, "

            for i in range(len(metrics)):
                temp = "s"+str(siteIDs[i]) + "p"+str(paramIDs[i])
                sql += "AVG(" + temp + ".sample) AS sample" + temp + ", "

            sql = sql[0:-2]
            sql += " FROM "
            for i in range(len(metrics)):
                temp = "s"+str(siteIDs[i]) + "p"+str(paramIDs[i])
                sql += "sri."+table+" AS " + temp + ", "

            sql = sql[0:-2]  
            sql += " WHERE "
            count = 0
            for i in range(len(metrics)):
                temp = "s"+str(siteIDs[i]) + "p"+str(paramIDs[i])
                if (count > 0):
                    sql += "AND "
                sql += temp + ".site_id="+str(siteIDs[i])+" "
                count += 1
                sql += "AND " + temp + ".parameter_id="+str(paramIDs[i])+" "

            for i in range(len(metrics)):
                temp = "s"+str(siteIDs[i]) + "p"+str(paramIDs[i])
                if i == 0:
                    sql += "AND " + temp + ".datetime <= " + str(end) + " "
                    sql += "AND " + temp + ".datetime >= " + str(start) + " "
                else:
                    sql += "AND " + temp + ".datetime = " + "s"+str(siteIDs[i-1])+"p"+str(paramIDs[i-1]) + ".datetime "
            sql += "GROUP BY date"

    ##        print sql

            cursor.execute(sql)

            num_fields = len(cursor.description)
            ##fieldnames = [i[0] for i in cursor.description]

            results = cursor.fetchall()
            all_data = []

            for i in range(num_fields-1):
                all_data += [{}]
                all_data[i][metrics[i]] = []
        
            counts = [0 for metric in metrics]
            for row in results:
                # ignore index 0, which is the date
                for i in range(num_fields-1):
                    # take out "None"
                    if (row[i+1] != None and time.mktime(row[0].timetuple()) != None):
                        all_data[i][metrics[i]] += [{}]
                        # use for sample
    ##                    all_data[i][metrics[i]][count]["x"] = row[0]
                        # use for daily average
                        all_data[i][metrics[i]][counts[i]]["x"] = time.mktime(row[0].timetuple())
                        all_data[i][metrics[i]][counts[i]]["y"] = row[i+1]
                        counts[i] += 1

            # disconnect from server
            db.close()

            #print all_data
            return json.dumps(all_data)






if __name__ == "__main__":
    worker = ZeroMQWorker()
    worker.connect()
    worker.run()
