%default INPUT_PATH 's3n://jpacker-dev/amazon_products/books_dense.out'
%default OUTPUT_PATH 's3n://jpacker-dev/amazon_products/books_dense_graph'

crawled             =   LOAD '$INPUT_PATH' USING PigStorage() 
                        AS (asin: chararray, title: chararray, maker: chararray, 
                            price: float, rating: float, raters: int,
                            links: {t: (asin: chararray)});

nodes               =   FOREACH crawled
                        GENERATE asin, title, maker, price, rating, raters;
vertices            =   FOREACH nodes GENERATE asin;
edges               =   FOREACH crawled
                        GENERATE asin AS from, FLATTEN(links) AS to;

explored_edges_jnd  =   JOIN vertices BY asin, edges BY to;
explored_edges      =   FOREACH explored_edges_jnd GENERATE from, to;

num_vertices        =   FOREACH (GROUP vertices ALL) GENERATE COUNT($1);

rmf $OUTPUT_PATH/num_vertices;
rmf $OUTPUT_PATH/nodes;
rmf $OUTPUT_PATH/edges;
STORE num_vertices INTO '$OUTPUT_PATH/num_vertices' USING PigStorage();
STORE nodes INTO '$OUTPUT_PATH/nodes' USING PigStorage();
STORE explored_edges INTO '$OUTPUT_PATH/edges' USING PigStorage();
