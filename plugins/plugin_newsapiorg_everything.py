# Author:   Jon-Paul Boyd
import logging
import requests

log = logging.getLogger(__name__)


class NewsApiEverything:
    """
    A data loader plugin for the NewsApi.org everything Search API.
    """

    def __init__(self, url, api_key):
        self.sep = '.'
        self.page = 0
        self.statusOK = 'ok'
        self.url = url
        self.api_key = api_key
        self.query = None
        self.response_format = '.json'

    def connect(self, inc_column=None, max_inc_value=None):
        log.debug('Incremental Column: %r', inc_column)
        log.debug('Incremental Last Value: %r', max_inc_value)
        if inc_column:
            raise ValueError('Incremental loading not supported.')

    def disconnect(self):
        """Disconnect from the source."""
        # Nothing to do
        pass

    def flatten_dict(self, dictionary):
        result = {}
        keyvalue = [iter(dictionary.items())]
        keys = []
        while keyvalue:
            for k, v in keyvalue[-1]:
                keys.append(k)
                if isinstance(v, dict):
                    keyvalue.append(iter(v.items()))
                    break
                else:
                    result[self.sep.join(keys)] = v
                    keys.pop()
            else:
                if keys:
                    keys.pop()
                keyvalue.pop()
        return result

    def getUrl(self):
        return '{0}?q={1}&apiKey={2}'.format(
            self.url, self.query, self.api_key
        )

    def getDataBatch(self, batch_size):
        results = []

        url = self.getUrl()
        response = requests.get(url)
        docs = response.json()

        try:
            status = docs['status']
        except KeyError:
            return

        if status != self.statusOK:
            return

        try:
            articles = docs['articles']
        except KeyError:
            return

        for article in articles:
            result = self.flatten_dict(article)
            if 'publishedAt' in result:
                result['publishedAt'] =  result['publishedAt'][0:10]
            results.append(result)
            if len(results) >= batch_size:
                yield results
                results = []

        if results:
            yield results

    def getSchema(self):
        """
        Return the schema of the dataset
        :returns a List containing the names of the columns retrieved from the
        source
        """

        #JP - Schema hardcoded from flattened dictionary manually referencing first returned result
        #Could easily employ flatten_dict to dynamically generate schema
        schema = [
            'source.id',
            'source.name',
            'author',
            'title',
            'description',
            'publishedAt',
            'content'
        ]

        return schema


