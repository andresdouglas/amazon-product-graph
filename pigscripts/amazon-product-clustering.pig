/**
 * amazon-product-clustering
 *
 * Required parameters:
 *
 * - INPUT_PATH Input path for script data (e.g. s3n://hawk-example-data/tutorial/excite.log.bz2)
 * - OUTPUT_PATH Output path for script data (e.g. s3n://my-output-bucket/amazon-product-clustering)
 */

REGISTER '../udfs/python/amazon-product-clustering.py' USING streaming_python AS amazon_product_clustering;
REGISTER '../udfs/python/pig_matrix.py' USING streaming_python AS pig_matrix;
REGISTER '../udfs/python/pig_graph.py' USING streaming_python AS pig_graph;

IMPORT '../macros/matrix.pig';
IMPORT '../macros/graph.pig';

edges               =   LOAD '$INPUT_PATH' USING PigStorage('\t') AS (v1: int, v2: int);
adj_matrix          =   AdjacencyMatrix(edges);
trans_matrix        =   NormalizeMatrix(adj_matrix, col);
trans_mat_squared   =   MatrixMultiply(trans_matrix, trans_matrix);

rmf $OUTPUT_PATH;
STORE trans_mat_squared INTO '$OUTPUT_PATH' USING PigStorage('\t');
