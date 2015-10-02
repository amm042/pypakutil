from Tags import *

class QueryLast(object):
    '''Query the last input. Only available for 2.1'''
    def __init__(self, tags):
        self.tags = tags

    def toQueryData(self):
        result = {}
        result["metric"] = self.tags.metric
        result["tags"] = self.tags.toTagData()
        return result