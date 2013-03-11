-- %default NODES_INPUT_PATH 's3n://jpacker-dev/amazon_products/fixtures/kites-nodes'
-- %default NODES_INPUT_PATH 's3n://jpacker-dev/amazon_products/fixtures/cathedral-nodes'
-- %default NODES_INPUT_PATH 's3n://jpacker-dev/amazon_products/books_graph/nodes'

-- %default EDGES_INPUT_PATH 's3n://jpacker-dev/amazon_products/fixtures/kites-edges'
-- %default EDGES_INPUT_PATH 's3n://jpacker-dev/amazon_products/fixtures/cathedral-edges'
-- %default EDGES_INPUT_PATH 's3n://jpacker-dev/amazon_products/books_graph/edges'

-- %default OUTPUT_PATH 's3n://jpacker-dev/amazon_products/fixtures/kites_clustering'`
-- %default OUTPUT_PATH 's3n://jpacker-dev/amazon_products/fixtures/cathedral_clustering'
-- %default OUTPUT_PATH 's3n://jpacker-dev/amazon_products/books_graph/clustering'

%default NODES_INPUT_PATH '/Users/jpacker/code/test/amazon_products/nodes'
%default EDGES_INPUT_PATH '/Users/jpacker/code/test/amazon_products/sample/edges_100k'
%default OUTPUT_PATH '/Users/jpacker/code/test/amazon_products/output/clustering'

%default INFLATION_PARAMETER '1.5'
%default EPSILON '0.01'
%default MIN_ACCEPTABLE_CLUSTER_SIZE '20'

REGISTER 's3n://jpacker-dev/jar/datafu-0.0.9.jar';
DEFINE Enumerate datafu.pig.bags.Enumerate('1');

IMPORT '../macros/matrix.pig';
IMPORT '../macros/graph.pig';

nodes       =   LOAD '$NODES_INPUT_PATH' USING PigStorage() 
                AS (asin: chararray, title: chararray);
                -- AS (asin: chararray, title: chararray, maker: chararray, price: float, rating: float, raters: int);
edges       =   LOAD '$EDGES_INPUT_PATH' USING PigStorage() 
                AS (from: chararray, to: chararray);

trans_mat           =   TransitionMatrixWithSelfLoops(edges);

iteration_1         =   MCLIterate(trans_mat, $INFLATION_PARAMETER, $EPSILON);
iteration_2         =   MCLIterate(iteration_1, $INFLATION_PARAMETER, $EPSILON);
iteration_3         =   MCLIterate(iteration_2, $INFLATION_PARAMETER, $EPSILON);
iteration_4         =   MCLIterate(iteration_3, $INFLATION_PARAMETER, $EPSILON);
iteration_5         =   MCLIterate(iteration_4, $INFLATION_PARAMETER, $EPSILON);
iteration_6         =   MCLIterate(iteration_5, $INFLATION_PARAMETER, $EPSILON);
iteration_7         =   MCLIterate(iteration_6, $INFLATION_PARAMETER, $EPSILON);
iteration_8         =   MCLIterate(iteration_7, $INFLATION_PARAMETER, $EPSILON);
iteration_9         =   MCLIterate(iteration_8, $INFLATION_PARAMETER, $EPSILON);
iteration_10        =   MCLIterate(iteration_9, $INFLATION_PARAMETER, $EPSILON);

enumerated_clusters =   GetEnumeratedClustersFromMCLResult(iteration_10);

clusters_with_size  =   FOREACH enumerated_clusters
                        GENERATE i, COUNT(cluster) AS size, cluster AS items;
acceptable_clusters =   FILTER clusters_with_size BY size >= $MIN_ACCEPTABLE_CLUSTER_SIZE;

clusters_flattened  =   FOREACH acceptable_clusters GENERATE i, FLATTEN(items) AS asin;
with_titles         =   JOIN clusters_flattened BY asin, nodes BY asin;
clusters_regrouped  =   GROUP with_titles BY i;
clusters_out        =   FOREACH clusters_regrouped GENERATE 
                                group AS i,
                                COUNT($1) AS size,
                                $1.($1, $3) AS items;

stats               =   FOREACH (GROUP clusters_out ALL)
                        GENERATE COUNT($1), AVG($1.size);

rmf $OUTPUT_PATH/clusters;
rmf $OUTPUT_PATH/stats;
STORE clusters_out INTO '$OUTPUT_PATH/clusters' USING PigStorage();
STORE stats INTO '$OUTPUT_PATH/stats' USING PigStorage();
