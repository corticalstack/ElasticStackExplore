# Author: Jon-Paul Boyd
# Python client demo for ES search
import logging
import logging.config
from os import path
import re
import coloured_text as ct
from elasticsearch import Elasticsearch

log_file_path = path.join(path.dirname(path.abspath(__file__)), 'logging.conf')
logging.config.fileConfig(log_file_path)


# Custom exceptions for finer error identification
class SearchFailed(Exception):
    pass


class Search:
    """
    This class is used to search Elasticsearch
    """
    def __init__(self):
        """
        Set initial values in constructor
        """
        self.searchtip = 'NOTE max 10 hits displayed. Body content limited to 70 chars. Type quit to terminate'
        self.search1 = '1 All news'
        self.search2 = '2 News between 2017-03-01 & 2017-03-02 inclusive'
        self.search3 = '3 News by author "Jerome Hudson" (keyword term search)'
        self.search4 = '4 News by author "Jerome Hudson" (keyword term search, filter, no score)'
        self.search5 = '5 News by author with name starting "Jer" (text search with ngram via custom analyzer)'
        self.search6 = '6 News by author "Guy Tazz" with fuzziness=2'
        self.search7 = '7 News matching phrase "Smells Like Teen Spirit"'
        self.search8 = '8 News by multi match search on terms "augmented" or "intelligence"'
        self.search9 = ('9 News between 2015-01-01 - 2018-12-31, body containing phrase "machine learning", '
                        'title containing term "artificial", "augmented" or "singularity" - Example of multi condition '
                        'with bool query')
        self.prompt = 'Select option '
        self.quit = 'quit'
        self.invalid = 'Invalid option'
        self.es = None
        self.es_cluster = None
        self.es_user_consume = '*****'
        self.es_user_consume_pwd = '*****'
        self.total_event_count = 0
        self.es_cluster_url = "https://ab93385654d74a0da876074a41d0c243.eu-central-1.aws.cloud.es.io:9243/"
        self.toggle = False

    @staticmethod
    def get_query_body1():
        return '{"query": {"match_all": {}}}'

    @staticmethod
    def get_query_body2():
        return '{"query": {"range": {"date_publication": {"gte": "2017-03-01", "lte": "2017-03-02"}}}}'

    @staticmethod
    def get_query_body3():
        return '{"query": {"term": {"author": "Jerome Hudson"}}}'

    @staticmethod
    def get_query_body4():
        return '{"query": {"constant_score": {"filter": {"term": {"author": "Jerome Hudson"}}}}}'

    @staticmethod
    def get_query_body5():
        return '{"query": {"match": {"author.search": "Jer"}}}'

    @staticmethod
    def get_query_body6():
        return '{"query": {"match": {"author": {"query": "Guy Tazz", "fuzziness": 2}}}}'

    @staticmethod
    def get_query_body7():
        return '{"query": {"match_phrase": {"body": {"query": "Smells Like Teen Spirit"}}}}'

    @staticmethod
    def get_query_body8():
        return '{"query": {"multi_match": {"query": "augmented intelligence", "fields": ["title", "body"]}}}'

    @staticmethod
    def get_query_body9():
        return ({"query": {"constant_score": {"filter":
                                                  {"bool": {"must": [{"range": {
                                                      "date_publication": {"gte": "2015-01-01", "lte": "2018-12-31"}}},
                                                                     {"match_phrase": {"body": "machine learning"}}],
                                                            "should":
                                                                [{"term": {"title.search": {"value": "artificial"}}},
                                                                 {"term": {"title.search": {"value": "augmented"}}},
                                                                 {"term": {"body": {"value": "singularity"}}}]}}}}})

    def output_options(self):
        print("")
        print(ct.Fore.BLUE + ct.Formatting.BOLD + self.search1 + ct.Formatting.RESET_ALL)
        print(ct.Fore.BLUE + ct.Formatting.BOLD + self.search2 + ct.Formatting.RESET_ALL)
        print(ct.Fore.BLUE + ct.Formatting.BOLD + self.search3 + ct.Formatting.RESET_ALL)
        print(ct.Fore.BLUE + ct.Formatting.BOLD + self.search4 + ct.Formatting.RESET_ALL)
        print(ct.Fore.BLUE + ct.Formatting.BOLD + self.search5 + ct.Formatting.RESET_ALL)
        print(ct.Fore.BLUE + ct.Formatting.BOLD + self.search6 + ct.Formatting.RESET_ALL)
        print(ct.Fore.BLUE + ct.Formatting.BOLD + self.search7 + ct.Formatting.RESET_ALL)
        print(ct.Fore.BLUE + ct.Formatting.BOLD + self.search8 + ct.Formatting.RESET_ALL)
        print(ct.Fore.BLUE + ct.Formatting.BOLD + self.search9 + ct.Formatting.RESET_ALL)
        print(ct.Fore.CYAN + ct.Formatting.BOLD + self.searchtip + ct.Formatting.RESET_ALL)
        print("")

    def es_connect(self):
        """
        Connect to Elasticstack cluster
        """
        try:
            self.es = Elasticsearch([self.es_cluster_url], use_ssl=False, http_auth=(self.es_user_consume,
                                                                                     self.es_user_consume_pwd))
        except Exception as e:
            logging.debug("Elasticstack connection error - {}".format(e.args))
            raise SearchFailed
        else:
            logging.debug("Connected to Elasticstack")

    def toggle_colour(self):
        """
        Flip-flop field display colour
        """
        self.toggle = not self.toggle
        if self.toggle:
            return ct.Fore.YELLOW
        else:
            return ct.Fore.WHITE

    def output_field(self, outfield, outlabel):
        print(self.toggle_colour(), end='')
        print(outlabel + '=' + str(outfield), end='')
        print(" " + ct.Formatting.RESET_ALL, end='')

    def output_hits(self, result):
        print(ct.Fore.MAGENTA + ct.Formatting.BOLD + 'Hits = ' + str(result['hits']['total']) + ct.Formatting.RESET_ALL)
        for hit in result['hits']['hits'][:10]:
            self.toggle = True
            if 'title' in hit['_source']:
                self.output_field(hit['_source']['title'], 'Title')

            if 'author' in hit['_source']:
                self.output_field(hit['_source']['author'], 'Author')

            if 'publication' in hit['_source']:
                self.output_field(hit['_source']['publication'], 'Publication')

            if 'date_publication' in hit['_source']:
                self.output_field(hit['_source']['date_publication'], 'Date Publication')

            if '_score' in hit:
                self.output_field(hit['_score'], 'Score')

            if 'body' in hit['_source']:
                self.output_field(hit['_source']['body'][:70], 'Body')

            print("")  # newline

    def parse(self, option):
        get_query_body = getattr(self, 'get_query_body' + option)
        result = self.es.search(index="news_*", body=get_query_body())
        self.output_hits(result)

    def main(self):
        """
        Entry into Search called from __main__
        """
        logging.debug("Search started")
        self.es_connect()
        user_option = ""

        while user_option != self.quit:
            self.output_options()
            user_option = input(ct.Fore.RED + ct.Formatting.BOLD + self.prompt + ct.Formatting.RESET_ALL)
            if user_option == self.quit:
                break
            if re.search('[a-zA-Z]', user_option):
                print(ct.Fore.RED + ct.Formatting.BOLD + self.invalid + ct.Formatting.RESET_ALL)
                continue
            if 1 <= int(user_option) <= 9:
                self.parse(user_option)
                continue
            print(ct.Fore.RED + ct.Formatting.BOLD + self.invalid + ct.Formatting.RESET_ALL)


if __name__ == '__main__':
    try:
        search = Search()
        search.main()
    except (SearchFailed, NameError, AttributeError) as e:
        logging.debug("Search execution failed - {}".format(e.args))
    else:
        logging.debug("Search completed")



