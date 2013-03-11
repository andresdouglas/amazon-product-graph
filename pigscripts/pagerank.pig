%default NODES_INPUT_PATH 's3n://jpacker-dev/amazon_products/fixtures/kites-nodes'
-- %default NODES_INPUT_PATH 's3n://jpacker-dev/amazon_products/fixtures/cathedral-nodes'
-- %default NODES_INPUT_PATH 's3n://jpacker-dev/amazon_products/books_graph/nodes'

%default EDGES_INPUT_PATH 's3n://jpacker-dev/amazon_products/fixtures/kites-edges'
-- %default EDGES_INPUT_PATH 's3n://jpacker-dev/amazon_products/fixtures/cathedral-edges'
-- %default EDGES_INPUT_PATH 's3n://jpacker-dev/amazon_products/books_graph/edges'

%default OUTPUT_PATH 's3n://jpacker-dev/amazon_products/fixtures/kites_pagerank'
-- %default OUTPUT_PATH 's3n://jpacker-dev/amazon_products/fixtures/kites_pagerank'
-- %default OUTPUT_PATH 's3n://jpacker-dev/amazon_products/books_graph/pagerank'

%default DAMPING_FACTOR '0.85'

IMPORT '../macros/matrix.pig';
IMPORT '../macros/graph.pig';

nodes               =   LOAD '$NODES_INPUT_PATH' USING PigStorage() 
                        AS (asin: chararray, title: chararray);
                        --    AS (asin: chararray, title: chararray, maker: chararray, 
                        --        price: float, rating: float, raters: int);
edges               =   LOAD '$EDGES_INPUT_PATH' USING PigStorage() 
                        AS (from: chararray, to: chararray);

vertices            =   FOREACH nodes GENERATE asin;
num_vertices        =   FOREACH (GROUP vertices ALL) GENERATE COUNT(vertices) AS N;
start_vec           =   FOREACH vertices 
                        GENERATE asin AS i, 1.0 / num_vertices.N AS val;

trans_mat           =   TransitionMatrix(edges);
damped_trans_mat    =   MatrixScalarProduct(trans_mat, $DAMPING_FACTOR);

iteration_1         =   PagerankIterate(start_vec, damped_trans_mat, num_vertices, $DAMPING_FACTOR);
iteration_2         =   PagerankIterate(iteration_1, damped_trans_mat, num_vertices, $DAMPING_FACTOR);
iteration_3         =   PagerankIterate(iteration_2, damped_trans_mat, num_vertices, $DAMPING_FACTOR);

pageranks           =   FOREACH iteration_3 GENERATE i AS asin, val AS pagerank;
pagerank_sum        =   FOREACH (GROUP pageranks ALL) 
                        GENERATE SUM(pageranks.pagerank) AS sum;

-- renormalize to mitigate accumulated numerical error
renormalized        =   FOREACH pageranks 
                        GENERATE asin, pagerank / pagerank_sum.sum AS pagerank;
with_titles         =   JOIN renormalized BY asin, nodes BY asin;
pageranks_out       =   FOREACH with_titles GENERATE $0, $1, nodes::title;

rmf $OUTPUT_PATH/pageranks;
rmf $OUTPUT_PATH/pagerank_sum;
STORE pageranks_out INTO '$OUTPUT_PATH/pageranks' USING PigStorage();
STORE pagerank_sum INTO '$OUTPUT_PATH/sum' USING PigStorage();
