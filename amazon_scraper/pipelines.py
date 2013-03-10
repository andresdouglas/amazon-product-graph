from scrapy.exceptions import DropItem

class AmazonProductPipeline(object):
    def __init__(self):
        self.seen = set()

    def process_item(self, item, spider):
        if item['asin'] in self.seen:
            #raise DropItem("Dropped duplicate product \"{}\" (ASIN: {})".format(item['title'], item['asin']))
            return
        elif item['price'] is None:
            #raise DropItem("Dropped affiliate product \"{}\" (ASIN: {})".format(item['title'], item['asin']))
            return
        else:
            self.seen.add(item['asin'])
            return item
