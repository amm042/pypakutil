import time
import logging 

class PutData(object):
    """Single put data"""
    def __init__(self, timestamp, value, tags):
        self.timestamp = timestamp
        self.value = value
        self.tags = tags.copy()
        self.metric = tags.metric
        self.logger = logging.getLogger("SensorAPI_API")

        if self.logger.isEnabledFor(logging.DEBUG):
            self.logger.debug("PutData object created. metric: {0}, timestamp = {1}, value = {2}".format(self.metric, self.timestamp, self.value))

    def toPutData(self):
        data = {}
        data["metric"] = self.metric
        data["timestamp"] = self.timestamp
        data["value"] = self.value
        data["tags"] = self.tags.toTagData()

        if self.logger.isEnabledFor(logging.DEBUG):
            self.logger.debug("Raw PutData created. metric: {0}, timestamp = {1}, value = {2}".format(self.metric, self.timestamp, self.value))

        return data

    def __repr__(self):
        result = "timestamp: {0}, value = {1}, metric: {2}. ".format(self.timestamp, self.value, self.metric)
        result += "Tags: {0}".format(self.tags)
        return result