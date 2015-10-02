import re
import logging

class QueryAggregator(object):
    """Query aggregator enum"""
    Sum = "sum"
    Min = "min"
    Max = "max"
    Average = "avg"
    StandardDeviation = "dev"

class QueryData(object):
    """Single query data"""
    def __init__(self, tags, aggregator, downSample = None, rate = None):
        '''Initialize the query
        '''
        self.tags = tags
        self.downSample = downSample
        self.rate = rate
        if aggregator == None:
            self.aggregator = QueryAggregator.Average
        else:
            self.aggregator  = aggregator
        self.logger = logging.getLogger("SensorAPI_API")

        if self.logger.isEnabledFor(logging.DEBUG):
            self.logger.debug("QueryData object created. metric: {0}, aggregator = {1}".format(self.tags.metric, self.aggregator))

    def toQueryData(self):
        result = {}
        result["metric"] = self.tags.metric
        result["tags"] = self.tags.toTagData()
        result["aggregator"] = self.aggregator
        if self.downSample != None:
            result["downsample"] = self.downSample.toDownSampleData()

        if self.rate != None:
            result["rate"] = self.rate

        if self.logger.isEnabledFor(logging.DEBUG):
            self.logger.debug("Raw QueryData created. metric: {0}, aggregator = {1}".format(self.tags.metric, self.aggregator))
        return result


    def __repr__(self):
        result = "Query Info: metric :{0}, tags: {1}, aggregator: {2}, downsample: {3}".format(self.tags.metric, self.tags, self.aggregator, self.downSample)
        return result


class DownSample:
    """Downsampling object for querying"""
    def __inif__(self, downsampleRate, aggregator):
        '''
        Initialize the downsample object.
        downsampleRate: number prefix with time abbr, e.g. 12m, 1h
        aggregator: QueryAffregator or a verified string, e.g. avg
        '''
        self.aggregator = aggregator
        m = re.search("\d+[s,m]", downsampleRate, aggregator)
        if m:
            self.downsampleRate = downsampleRate
        else:
            self.downsampleRate = "1m"

    def toDownSampleData(self):
        '''
        Return the downsample string that OpenTSDB can understand
        '''
        return "{0}-{1}".format(self.downsampleRate, self.aggregator)