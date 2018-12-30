# Author:   Jon-Paul Boyd
# Date:     05/12/2018
import argparse
import logging
import requests

log = logging.getLogger(__name__)


class NYTimesSource(object):
    """
    A data loader plugin for the NY Times API.
    """

    def __init__(self):
        self.sep = '.'
        self.page = 0
        self.pagelimit = 10  # Page limit supports testing
        self.numpages = 0
        self.statusOK = 'OK'
        self.url = 'https://api.nytimes.com/svc/search/v2/articlesearch'
        self.api_key = '9194b40ad1574f42993aba8ff8637f7e'
        self.query = 'Silicon Valley'
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
        #Could easily employ flatten_dict to dynamically generate dictionary
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


if __name__ == '__main__':
    config = {
        'url': 'https://api.nytimes.com/svc/search/v2/articlesearch',
        'api_key': '9194b40ad1574f42993aba8ff8637f7e',
        'query': 'Silicon Valley',
        'response_format': '.json'
    }
    source = NYTimesSource()

    # This looks like an argparse dependency - but the Namespace class is just
    # a simple way to create an object holding attributes.
    source.args = argparse.Namespace(**config)

    for idx, batch in enumerate(source.getDataBatch(3)): # Set to 3 testing batch & yield as API page limit 10 articles
        print('{1} Batch of {0} items'.format(len(batch), idx))
        for item in batch:
            print('  - {0} - {1}'.format(item['_id'], item['headline.main']))


