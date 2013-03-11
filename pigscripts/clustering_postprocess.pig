/*
 * NODES_INPUT_PATH
 * MCL_RESULT_PATH
 * CLUSTERS_OUTPUT_PATH
 * STATS_OUTPUT_PATH
 */

REGISTER 's3n://jpacker-dev/jar/datafu-0.0.9.jar';
DEFINE Enumerate datafu.pig.bags.Enumerate('1');

IMPORT '../macros/matrix.pig';
IMPORT '../macros/graph.pig';

nodes       =   LOAD '$NODES_INPUT_PATH' USING PigStorage() 
                AS (asin: chararray, title: chararray);
                -- AS (asin: chararray, title: chararray, maker: chararray, price: float, rating: float, raters: int);

mcl_result  =   LOAD '$MCL_RESULT_PATH' USING PigStorage() AS (row: chararray, col: chararray, val: double);

enumerated_clusters =   GetEnumeratedClustersFromMCLResult(mcl_result);

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

rmf $CLUSTERS_OUTPUT_PATH;
rmf $STATS_OUTPUT_PATH;
STORE clusters_out INTO '$CLUSTERS_OUTPUT_PATH' USING PigStorage();
STORE stats INTO '$STATS_OUTPUT_PATH' USING PigStorage();
