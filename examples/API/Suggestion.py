class Suggestion:
    def __init__(self, queryType, queryString, max = 25):
        if type(queryType) is SuggestionType:
            self.queryType = queryType
        else:
            self.queryType = SuggestionType.metrics
        self.queryString = queryString
        self.max = max

class SuggestionType(object):
    metrics = "metrics"
    tagKey = "tagk"
    tagValue = "tagv"
