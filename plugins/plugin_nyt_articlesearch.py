# Author:   Jon-Paul Boyd
import logging
import requests

log = logging.getLogger(__name__)


class NYTimesSource:
    """
    A data loader plugin for the New York Times Article Search API.
    """

    def __init__(self, url, api_key):
        self.sep = '.'
        self.page = 0
        self.pagelimit = 1  # Page limit supports testing
        self.numpages = 0
        self.statusOK = 'OK'
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
        return '{0}{1}?api-key={2}&q={3}&page={4}'.format(
            self.url, self.response_format, self.api_key, self.query, self.page
        )

    def setNumPages(self):
        url = self.getUrl()
        response = requests.get(url)
        docs = response.json()
        try:
            hits = docs['response']['meta']['hits']
        except KeyError:
            return

        self.numpages = (hits / 10) - 1
        if self.numpages > self.pagelimit:
            self.numpages = self.pagelimit

    def getDataBatch(self, batch_size):
        results = []
        self.setNumPages()

        while self.page < self.numpages:
            url = self.getUrl()
            response = requests.get(url)
            docs = response.json()

            try:
                status = docs['status']
            except KeyError:
                continue

            if status != self.statusOK:
                continue

            try:
                articles = docs['response']['docs']
            except KeyError:
                continue

            for article in articles:
                result = self.flatten_dict(article)
                if 'pub_date' in result:
                    result['pub_date'] =  result['pub_date'][0:10]
                results.append(result)
                if len(results) >= batch_size:
                    yield results
                    results = []

            if results:
                yield results

            self.page += 1


    def getSchema(self):
        """
        Return the schema of the dataset
        :returns a List containing the names of the columns retrieved from the
        source
        """

        #JP - Schema hardcoded from flattened dictionary manually referencing first returned result
        #Could easily employ flatten_dict to dynamically generate schema
        schema = [
            'web_url',
            'snippet',
            'multimedia',
            'headline.main',
            'headline.kicker',
            'headline.content_kicker',
            'headline.print_headline',
            'headline.name',
            'headline.seo',
            'headline.sub',
            'keywords',
            'pub_date',
            'byline.original',
            'document_type',
            'type_of_material',
            '_id',
            'word_count',
            'score'
        ]

        return schema


