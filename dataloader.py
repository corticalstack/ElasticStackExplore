import json
import importlib
from elasticsearch import helpers, Elasticsearch


class DataLoader():

    def __init__(self):
        self.config = None
        self.es = None
        self.es_cluster = None
        self.es_user_ingest = None
        self.es_user_ingest_pwd = None

    def load_set_config(self):
        with open("config/config.json", "r") as config_file:
            self.config = json.load(config_file)

        try:
            self.es_cluster = self.config['dataloader']['elasticsearch']['cluster_url']
            self.es_user_ingest = self.config['dataloader']['elasticsearch']['user_ingest']
            self.es_user_ingest_pwd = self.config['dataloader']['elasticsearch']['user_ingest_pwd']
        except KeyError:
            pass

    def es_connect(self):
        self.es = Elasticsearch([self.es_cluster], http_auth=(self.es_user_ingest, self.es_user_ingest_pwd))

    def get_plugin_class_instance(self, module, cls):
        plugin_module_name = "plugins." + module
        plugin_module = importlib.import_module(plugin_module_name)
        class_ = getattr(plugin_module, cls)
        return class_()

    def fieldmap(self, event, cls, fieldmap):
        target_event = {}
        schema = cls.getSchema()
        for key in fieldmap:
            if fieldmap[key] in schema:
                try:
                    target_event[key] = event[fieldmap[key]]
                except KeyError: #Handle field not in event source
                    pass

        return target_event

    def es_index(self, event, plugin):
        try:
            index = plugin['index_prefix'] + event[plugin['index_suffix_field']]
        except KeyError:
            index = plugin['index_prefix'] + plugin['index_default_suffix']

        if 'publication' not in event:
            event['publication'] = plugin['publication_default']

        if 'date_publication' not in event:
            event['date_publication'] = plugin['date_publication_default']

        self.es.index(index=index, doc_type='doc', id=event['id'], body=event)

    def ingest(self):
        try:
            for p in self.config['dataloader']['plugin']:
                p_class = self.get_plugin_class_instance(p['module'], p['module_class'])
                for events in p_class.getDataBatch(3):
                    for event in events:
                        print(p)
                        print(p['fieldmap'])
                        target_event = self.fieldmap(event, p_class, p['fieldmap'])
                        self.es_index(target_event, p)

        except KeyError:
            pass


if __name__ == '__main__':
    dataloader = DataLoader()
    dataloader.load_set_config()
    dataloader.es_connect()
    dataloader.ingest()


