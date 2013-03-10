%default INPUT_PATH 's3n://jpacker-dev/amazon_products/books.out'
%default OUTPUT_PATH 's3n://jpacker-dev/amazon_products/books_graph'

crawled		=		LOAD '$INPUT_PATH' USING PigStorage() 
					AS (asin: chararray, title: chararray, maker: chararray, 
					    price: float, rating: float, raters: int,
					    links: {t: (asin: chararray)});

nodes		=		FOREACH crawled
					GENERATE asin, title, maker, price, rating, raters;

edges		=		FOREACH crawled
					GENERATE asin, FLATTEN(links);

rmf $OUTPUT_PATH/nodes;
rmf $OUTPUT_PATH/edges;
STORE nodes INTO '$OUTPUT_PATH/nodes' USING PigStorage();
STORE edges INTO '$OUTPUT_PATH/edges' USING PigStorage();
