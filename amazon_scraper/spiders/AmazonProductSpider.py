from __future__ import absolute_import, division, print_function, unicode_literals

from HTMLParser import HTMLParser

import re

from scrapy.conf import settings
from scrapy.http import Request
from scrapy.spider import BaseSpider
from scrapy.selector import HtmlXPathSelector

from amazon_scraper.items import AmazonProduct

class AmazonProductSpider(BaseSpider):
    name="amazon-products"
    allowed_domains = ["amazon.com"]
    start_urls = settings['START_URLS']

    base_url = "http://www.amazon.com/dp/"
    max_num_product_links = settings.get('MAX_NUM_PRODUCT_LINKS', 100)

    slash_pattern = re.compile('/')
    html_tag_pattern = re.compile('<.*?>')
    price_pattern = re.compile('\$([\d\.]*)')
    rating_pattern = re.compile('(\d\.?\d?) out of \d\.?\d? stars')
    raters_pattern = re.compile('(\d[\d,]*) customer reviews')
    comma_pattern = re.compile(',')

    htmlParser = HTMLParser()

    def parse(self, response):
        hxs = HtmlXPathSelector(response)

        product = AmazonProduct();

        asin = self.slash_pattern.split(response.url)[-1]
        product['asin'] = asin

        try:
            title = hxs.select('//span[@id="btAsinTitle"][1]').extract()[0]
            product['title'] = self.htmlParser.unescape(re.sub(self.html_tag_pattern, '', title).strip())
        except:
            product['title'] = None

        try:
            maker = hxs.select('//div[@class="buying"]/h1[contains(@class,"parseasinTitle")]/following-sibling::span')[0].extract()
            product['maker'] = self.htmlParser.unescape(re.sub(self.html_tag_pattern, '', maker).strip().replace("by\xa0", ""))
        except:
            product['maker'] = None

        try:
            product['price'] = float(
                hxs.select('//table[@class="product"]//span[@id="listPriceValue"]')[0].
                    re(self.price_pattern)[0]
            )
        except:
            product['price'] = None

        if product['price'] is None:
            try:
                product['price'] = float(
                    hxs.select('//table[@class="product"]//span[@id="actualPriceValue"]')[0].
                    re(self.price_pattern)[0]
                )
            except:
                pass

        ratingSpans = hxs.select('//span[@class="crAvgStars"]')

        try:
            product['rating'] = float(
                ratingSpans[0].select('span[@name="' + asin + '"]')[0].
                re(self.rating_pattern)[0]
            )
        except:
            product['rating'] = None

        try:
            product['raters'] = int(
                ratingSpans[0].re(self.raters_pattern)[0].replace(',', '')
            )
        except:
            product['raters'] = None

        try:
            links = hxs.select('//div[@id="purchaseSimsData"]')[0].re("<div.*>(.*)</div>")[0]
            links = self.comma_pattern.split(links)[:self.max_num_product_links]
            product['product_links'] = links
        except:
            product['product_links'] = []

        yield product
        for url in [self.base_url + link_asin for link_asin in product['product_links']]:
            yield Request(url, callback=self.parse)
