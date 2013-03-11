%default NODES_INPUT_PATH 's3n://jpacker-dev/amazon_products/fixtures/kites-nodes'
-- %default NODES_INPUT_PATH 's3n://jpacker-dev/amazon_products/fixtures/cathedral-nodes'
-- %default NODES_INPUT_PATH 's3n://jpacker-dev/amazon_products/books_graph/nodes'

%default EDGES_INPUT_PATH 's3n://jpacker-dev/amazon_products/fixtures/kites-edges'
-- %default EDGES_INPUT_PATH 's3n://jpacker-dev/amazon_products/fixtures/cathedral-edges'
-- %default EDGES_INPUT_PATH 's3n://jpacker-dev/amazon_products/books_graph/edges'

%default OUTPUT_PATH 's3n://jpacker-dev/amazon_products/fixtures/kites_clustering'
-- %default OUTPUT_PATH 's3n://jpacker-dev/amazon_products/fixtures/cathedral_clustering'
-- %default OUTPUT_PATH 's3n://jpacker-dev/amazon_products/books_graph/clustering'

%default INFLATION_PARAMETER '2.0'
%default EPSILON '0.01'

REGISTER 's3n://jpacker-dev/jar/datafu-0.0.9.jar';
DEFINE Enumerate datafu.pig.bags.Enumerate('1');

IMPORT '../macros/matrix.pig';
IMPORT '../macros/graph.pig';

nodes		=	LOAD '$NODES_INPUT_PATH' USING PigStorage() 
				AS (asin: chararray, title: chararray);
				-- AS (asin: chararray, title: chararray, maker: chararray, price: float, rating: float, raters: int);
edges		=	LOAD '$EDGES_INPUT_PATH' USING PigStorage() 
				AS (from: chararray, to: chararray);

trans_mat 	= 	TransitionMatrixWithSelfLoops(edges);

iteration_1 			=	MCLIterate(trans_mat, $INFLATION_PARAMETER, $EPSILON);
iteration_2 			=	MCLIterate(iteration_1, $INFLATION_PARAMETER, $EPSILON);
iteration_3 			=	MCLIterate(iteration_2, $INFLATION_PARAMETER, $EPSILON);
/*
iteration_4 			=	MCLIterate(iteration_3, $INFLATION_PARAMETER, $EPSILON);
iteration_5 			=	MCLIterate(iteration_4, $INFLATION_PARAMETER, $EPSILON);
iteration_6 			=	MCLIterate(iteration_5, $INFLATION_PARAMETER, $EPSILON);
iteration_7 			=	MCLIterate(iteration_6, $INFLATION_PARAMETER, $EPSILON);
*/

by_row					=	GROUP iteration_3 BY row;
clusters_with_dups		=	FOREACH by_row GENERATE $1.col AS cluster;
clusters_dups_ordered	=	FOREACH clusters_with_dups {
								ordered = ORDER cluster BY $0 ASC;
								GENERATE ordered AS cluster;
							}
clusters				=	DISTINCT clusters_dups_ordered;

clusters_enumerated		=	FOREACH (GROUP clusters ALL)
							GENERATE FLATTEN(Enumerate(clusters)) AS (cluster, idx);
clusters_flattened		=	FOREACH clusters_enumerated GENERATE idx, FLATTEN(cluster) AS asin;
with_titles				=	JOIN clusters_flattened BY asin, nodes BY asin;
clusters_regrouped		=	GROUP with_titles BY idx;
clusters_out			=	FOREACH clusters_regrouped GENERATE 
								group AS cluster_idx,
								COUNT($1) AS num_items,
								$1.($1, $3) AS items;

stats					=	FOREACH (GROUP clusters_out ALL)
							GENERATE COUNT($1), AVG($1.num_items);

debug					=	VisualizeMatrix(iteration_3, 'col');

rmf $OUTPUT_PATH/clusters;
rmf $OUTPUT_PATH/stats;
rmf $OUTPUT_PATH/debug;
STORE clusters_out INTO '$OUTPUT_PATH/clusters' USING PigStorage();
STORE stats INTO '$OUTPUT_PATH/stats' USING PigStorage();
STORE debug INTO '$OUTPUT_PATH/debug' USING PigStorage();
