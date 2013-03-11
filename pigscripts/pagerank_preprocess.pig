/*
 * NODES_INPUT_PATH
 * EDGES_INPUT_PATH
 * VECTOR_OUTPUT_PATH
 * MATRIX_OUTPUT_PATH
 */

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
                        GENERATE asin AS i, 1.0 / (double)num_vertices.N AS val;

num_vertices_copy   =   FOREACH num_vertices GENERATE *;

trans_mat           =   TransitionMatrix(edges);
damped_trans_mat    =   MatrixScalarProduct(trans_mat, $DAMPING_FACTOR);

rmf $VECTOR_OUTPUT_PATH;
rmf $MATRIX_OUTPUT_PATH;
rmf $NUM_VERTICES_OUTPUT_PATH;
STORE start_vec INTO '$VECTOR_OUTPUT_PATH' USING PigStorage();
STORE damped_trans_mat INTO '$MATRIX_OUTPUT_PATH' USING PigStorage();
STORE num_vertices_copy INTO '$NUM_VERTICES_OUTPUT_PATH' USING PigStorage();
