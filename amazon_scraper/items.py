from scrapy.item import Item, Field

class AmazonProduct(Item):
    asin = Field()
    title = Field()
    maker = Field()
    price = Field()
    rating = Field()
    raters = Field()
    product_links = Field()
    pass
