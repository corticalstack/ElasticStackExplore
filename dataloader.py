# Author: Jon-Paul Boyd
# Python client web scraping news then ingesting into ES
import logging
import logging.config
from os import path
import json
import importlib
from elasticsearch import Elasticsearch

log_file_path = path.join(path.dirname(path.abspath(__file__)), 'logging.conf')
logging.config.fileConfig(log_file_path)

# Custom exceptions for finer error identification
class DataloaderFailed(Exception):
    pass


class ConfigFileError(Exception):
    pass


class ConfigKeyError(Exception):
    pass


class PluginModuleNotFoundError(Exception):
    pass


class PluginModuleClassNotFoundError(Exception):
    pass

class DataLoader:
    """
    This class is used to scrape API sourced data and ingest into Elasticsearch
    """
    def __init__(self):
        """
        Set initial values in constructor
        """
        self.config_file = "config/config.json"
        self.config = None
        self.es = None
        self.es_cluster = None
        self.es_user_ingest = None
        self.es_user_ingest_pwd = None
        self.total_event_count = 0

    def main(self):
        """
        Entry into Dataloader called from __main__
        """
        logging.info("Dataloader started")

        # Load the config driving the dataload which is persisted in JSON file, handling specific exceptions
        try:
            dataloader.load_set_config()
        except ConfigFileError as e:
            logging.debug("Configuration file error - {}".format(e.args[1]))
            raise DataloaderFailed
        except ConfigKeyError as e:
            logging.debug("Configuration key error with key - {}".format(e.args[1]))
            raise DataloaderFailed

        # Make connection to Elasticstack (ES) cluster
        dataloader.es_connect()

        # For each plugin scrape data according to configured query(s) and load into ES
        dataloader.es_plugin_process()
        logging.info("Total events indexed - {}".format(self.total_event_count))

    def load_set_config(self):
        """
        Load JSON config file then set ES connection parameters
        """
        try:
            with open(self.config_file, "r") as config_file:
                self.config = json.load(config_file)
        except FileNotFoundError as e:
            raise ConfigFileError(None, self.config_file)
        else:
            try:
                self.es_cluster = self.config['dataloader']['elasticsearch']['cluster_url']
                self.es_user_ingest = self.config['dataloader']['elasticsearch']['user_ingest']
                self.es_user_ingest_pwd = self.config['dataloader']['elasticsearch']['user_ingest_pwd']
            except KeyError as e:
                raise ConfigKeyError(None, e.args[0])
            else:
                logging.info("Successfully loaded config")

    def get_plugin_class_instance(self, plugin):
        """
        For plugin - dynamically load module and return instance of module class that scrapes data
        """
        plugin_module_name = "plugins." + plugin['module']
        try:
            plugin_module = importlib.import_module(plugin_module_name)
        except ModuleNotFoundError as e:
            raise PluginModuleNotFoundError(None, e.args[0])

        try:
            class_ = getattr(plugin_module, plugin['module_class'])
        except AttributeError as e:
            raise PluginModuleClassNotFoundError(None, e.args[0])

        return class_(plugin['url'], plugin['api_key'])

    def fieldmap(self, event, cls, fieldmap):
        """
        Use plugin API schema field list to map to target ES index fields, returning an event to index
        """
        target_event = {}
        schema = cls.getSchema()
        for key in fieldmap:
            if fieldmap[key] in schema:
                try:
                    target_event[key] = event[fieldmap[key]]
                except KeyError: #Handle field not in event source
                    pass

        return target_event

    def es_connect(self):
        """
        Connect to Elasticstack cluster
        """
        try:
            self.es = Elasticsearch([self.es_cluster], use_ssl=False, http_auth=(self.es_user_ingest,
                                                                                 self.es_user_ingest_pwd))
        except Exception as e:
            logging.debug("Elasticstack connection error - {}".format(e.args))
            raise DataloaderFailed
        else:
            logging.info("Connected to Elasticstack")

    def es_index(self, event, plugin):
        """
        Index a scraped event into ES
        """
        try:
            index = plugin['index_prefix'] + event[plugin['index_suffix_field']]
        except KeyError:
            index = plugin['index_prefix'] + plugin['index_default_suffix']

        # Use defaults if none from scraped event
        if 'publication' not in event:
            event['publication'] = plugin['publication_default']

        if 'date_publication' not in event:
            event['date_publication'] = plugin['date_publication_default']

        if 'year' not in event:
            event['year'] = plugin['year_default']

        if 'yearmonth' not in event:
            event['yearmonth'] = plugin['yearmonth_default']

        # Use source id if available, else let ES determine index id
        if 'id' in event:
            self.es.index(index=index, doc_type='doc', id=event['id'], body=event)
        else:
            self.es.index(index=index, doc_type='doc', body=event)

    def es_plugin_process(self):
        """
        Process each configured plugin
        """
        try:
            for p in self.config['dataloader']['plugin']:
                if p['enabled']:  # Only if plugin enabled
                    logging.info("Processing started for {} plugin".format(p['api']))
                    try:
                        p_class = self.get_plugin_class_instance(p)  # Instantiate class handling plugin
                    except (PluginModuleNotFoundError,  PluginModuleClassNotFoundError) as e:
                        logging.debug("Skipping plugin, module/class not found - {}".format(e.args[1]))
                        continue

                    for q in p['query']:  # Loop plugin query list
                        event_count = 0
                        p_class.query = q
                        for events in p_class.getDataBatch(10):
                            for event in events:
                                target_event = self.fieldmap(event, p_class, p['fieldmap'])  # Map source -> tgt fields
                                self.es_index(target_event, p)  # Index event in ES
                                event_count += 1
                                self.total_event_count += 1
                        logging.info("{} events scraped for query '{}'".format(event_count, q))
                    logging.info("Processing complete for {} plugin".format(p['api']))

        except KeyError as e:
            logging.debug("Plugin error - {}".format(e.args))
            raise DataloaderFailed


if __name__ == '__main__':
    try:
        dataloader = DataLoader()
        dataloader.main()
    except (DataloaderFailed, NameError, AttributeError) as e:
        logging.debug("Dataloader execution failed - {}".format(e.args))
    else:
        logging.info("Dataloader completed")



