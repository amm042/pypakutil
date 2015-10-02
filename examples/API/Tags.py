class Tags:
    '''
    Tag object that contains the entry information for each data point
    '''
    def __init__(self, metric):
        '''
        Initialize the Tag object.
        metric: metric name, string.
        '''
        self.__tags = {}
        self.metric = metric

    def addTag(self, tagName, tagValue):
        '''
        Add tags to the object
        tagName: string value of tag name, i.e. key
        tagValue: string value of tag value
        '''
        if tagName not in self.__tags:
            self.__tags[tagName] = tagValue

    def toTagData(self):
        '''Convert to the tag object'''
        return self.__tags

    def getTags(self):
        return self.__tags.keys()
    
    def clearTags(self):
        self.__tags.clear()

    def updateTags(self, tagName, tagValue):
        if tagName in self.__tags:
            self.__tags[tagName] = tagValue

    def copy(self):
        '''
        Deep copy of a tag object
        '''
        new = Tags(self.metric)
        new.__tags = self.__tags.copy()
        return new


    def __repr__(self):
        result = ""
        for k in self.__tags:
            result += "key: {0} value: {1}, ".format(k, self.__tags[k])
        return result[0:-2]
