from scrapy import log
from scrapy.conf import settings
from scrapy.contrib.exporter import BaseItemExporter

class PigStorageExporter(BaseItemExporter):
    def __init__(self, output_file, **kwargs):
        self._configure(kwargs)
        self.out = output_file
        self.fields = settings['PIGSTORAGE_FIELDS']
        self.delimiter = settings.get('PIGSTORAGE_DELIMITER', '\t')

    def export_item(self, item):
        if item is not None:
            self.out.write(self.delimiter.join([self._serialize(item[field]) for field in self.fields]) + '\n')

    def _serialize(self, obj):
        if isinstance(obj, list):
            return '{' + ','.join([('(' + self._serialize(elem) + ')') for elem in obj]) + '}'
        elif isinstance(obj, tuple):
            return '(' + ','.join([self._serialize(elem) for elem in obj]) + ')'
        elif isinstance(obj, dict):
            return '[' + ','.join([(self._serialize(k) + '#' + self._serialize(v)) for k, v in obj.iteritems()]) +']'
        elif isinstance(obj, unicode):
            return obj.encode('utf-8')
        elif obj is not None:
            return str(obj)
        else:
            return ''
