import Queue
import time
import logging
from SensorAPI import SensorAPI
from API import *

class SensorClient:
    '''An easy to use sensor client for OpenTSDB'''

    def pushToBuffer(self, timestamp, value, tags):
        if timestamp == None: timestamp = self.nowMS()
        self.__queue.put(PutData(timestamp, value, tags))
        self.__queue.task_done()


    def batch(self):
        '''
        Submit all the pending data points to the server
        '''
        putDatas = []
        while not self.__queue.empty():
            putDatas.append(self.__queue.get())
        self.__queue.join()
        return self.api.multiplePut(putDatas)
        

    def __init__(self):
        '''
        Initialize the sensor client with given configuration

        conf: Client.Configuration
        '''
        self.api = SensorAPI()
        self.__queue = Queue.Queue()

        self.logger = logging.getLogger("SensorAPI_API")

        self.logger.info("SensorAPI created. Host IP: {0}, port: {1}".format(self.api._conf.getQueueHost(), self.api._conf.getQueuePort()))

    def singlePut(self, timestamp, value, tags):
        '''
        Put single data point into OpenTSDB.
        Returns feedback for a single put

        value: string, int, float, boolean
        tags: API.Tags
        timestamp: time in int (millisecond precision). Leaving it None will use the current system time.
            Note: Python built-in time.time() function returns a float in seconds. Using now() or getTimestamp(time) function is recommended
        '''
        if timestamp == None: timestamp = self.nowMS()
        putData = PutData(timestamp, value, tags)
        return self.api.singlePut(putData)

    def multiplePut(self, valueTuples):
        '''
        Put multiple data points into OpenTSDB.
        Returns feedback for multiple put

        valueTuples: list<(timestamp, value, tags)>
        '''
        # TODO: Add data validation
        datas = []
        for tup in valueTuples:
            datas += [PutData(tup[0], tup[1], tup[2].copy())]

        return self.api.multiplePut(datas)

    def nowMS(self):
        '''
        Returns the current timestamp in UTC time with millisecond precision
        '''
        from datetime import datetime
        return self.getUTCTimestampMS(datetime.utcnow())

    def nowS(self):
        '''
        Returns the current timestamp in UTC time with second precision
        '''
        from datetime import datetime
        return self.getUTCTimestampS(datetime.utcnow())
        pass

    def singleQuery(self, start, end, tags, aggregator = None, downSample = None):
        '''
        Returns the query result in JSON from given start time, end time, and tags

        start: start timestamp to query. Standard int in millisecond precision.
        end: end timestamp to query. Standard int in millisecond precision.
        tags: API.Tags
        aggregator: API.QueryAggregator, enum. If value not given, the API will use average by default
        downSample: API.DownSample
        '''
        query = QueryData(tags, aggregator, downSample)
        return self.api.singleQuery(start, end, query)

    def getUTCTimestampMS(self, dateTime):
        '''
        Returns the standard timestamp with millisecond precision from given time
        Note: for database performance concern, use second precision if possible
        dateTime: Python DateTime object
        '''
        import calendar
        return int(calendar.timegm(dateTime.timetuple()) * 1000 + dateTime.microsecond / 1000)

    def getUTCTimestampS(self, dateTime):
        '''
        Returns the standard timestamp with second precision from given time
        Note: For database performance concern, using second precision is recommended

        dateTime: Python DateTime object
        '''
        import calendar
        return int(calendar.timegm(dateTime.timetuple()))

    def singleQueryLast(self, tags):
        '''
        Returns the last timestamp and value from given tags

        tags: API.Tags
        '''
        last = QueryLast(tags)
        return self.api.singleQueryLast(last)

    def multipleQueryLast(self, tagsList):
        '''
        Returns the last timestamps and values from given tags list

        tagsList: list<API.Tags>
        '''
        lastList=[]
        for tags in tagsList:
            lastList += [tags]
        return self.api.multipleQueryLast(lastList)

    def postQuery(self, data):
        return self.api._postRequest("query", data)
    
    def postQueryLast(self, data):
        return self.api._postRequest("query/last", data)

    def lookup(self, data):
        return self.api._postRequest("search/lookup", data)

    def sendData(self, data):
        return self.api.sendData(data)

    def search(self, tags):
        '''
        Returns the result of time series search
        tags: API.Tags
        Note: wildcard character works here. For example, you can use * as tag or metric value
        '''
        return self.api.search(tags)

