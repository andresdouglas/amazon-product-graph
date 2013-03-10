BOT_NAME = 'flurgoblurg'

SPIDER_MODULES = ['amazon_scraper.spiders']
NEWSPIDER_MODULE = 'amazon_scraper.spiders'

MAX_NUM_PRODUCT_LINKS = 25

START_URLS = [
    "http://www.amazon.com/dp/1593080204", # Pride & Prejudice
    "http://www.amazon.com/dp/1907523650", # Mrs Dalloway
    "http://www.amazon.com/dp/0848817540", # A Passage to India

    "http://www.amazon.com/dp/0451526341", # Animal Farm
    "http://www.amazon.com/dp/0385333846", # Slaughterhouse Five
    "http://www.amazon.com/dp/0486264645", # Heart of Darkness

    "http://www.amazon.com/dp/0486280616", # Adventures of Huckleberry Finn
    "http://www.amazon.com/dp/0140177396", # Of Mice and Men
    "http://www.amazon.com/dp/0385333846", # The Great Gatsby

    "http://www.amazon.com/dp/059035342X", # Harry Potter
    "http://www.amazon.com/dp/0525478817", # The Fault in Our Stars
    "http://www.amazon.com/dp/0316066524", # Infinite Jest

    "http://www.amazon.com/dp/1416556966", # The Lathe of Heaven
    "http://www.amazon.com/dp/0062080237", # American Gods
    "http://www.amazon.com/dp/0553386794", # Game of Thrones

    "http://www.amazon.com/dp/0812550706", # Ender's Game
    "http://www.amazon.com/dp/0441014100", # Starship Troopers
    "http://www.amazon.com/dp/0441013597"  # Dune
]

ITEM_PIPELINES = [
    'amazon_scraper.pipelines.AmazonProductPipeline'
]

FEED_EXPORTERS = {
    'pigstorage': 'amazon_scraper.exporters.PigStorageExporter'
}
FEED_FORMAT = 'pigstorage'
FEED_URI = 's3://jpacker-dev/amazon_products/test.out'

AWS_ACCESS_KEY_ID = 'AKIAJ6JQIGSQ7MUX33OA'
AWS_SECRET_ACCESS_KEY = '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'

PIGSTORAGE_FIELDS = ['asin', 'title', 'maker', 'price', 'rating', 'raters', 'product_links']
PIGSTORAGE_DELIMITER = '\t'

LOG_LEVEL = 'INFO'
#LOG_FILE = '/tmp/scrapy/amazon_scraper.log'

CONCURRENT_REQUESTS = 100
CONCURRENT_REQUESTS_PER_DOMAIN = 100

COOKIES_ENABLED = False
REDIRECT_ENABLED = False
RETRY_ENABLED = False

# Breadth-first-crawl
DEPTH_PRIORITY = 1
SCHEDULER_DISK_QUEUE = 'scrapy.squeue.PickleFifoDiskQueue'
SCHEDULER_MEMORY_QUEUE = 'scrapy.squeue.FifoMemoryQueue'

# Crawl responsibly by identifying yourself (and your website) on the user-agent
USER_AGENT = 'flurgoblurg (+http://www.sky.net)'
