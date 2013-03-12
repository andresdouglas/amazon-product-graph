/*
 * EDGES_INPUT_PATH
 * VECTOR_OUTPUT_PATH
 * MATRIX_OUTPUT_PATH
 */

IMPORT '../macros/matrix.pig';
IMPORT '../macros/graph.pig';

edges               =   LOAD '$EDGES_INPUT_PATH' USING PigStorage() 
                        AS (from: chararray, to: chararray);

edge_destinations   =   FOREACH edges GENERATE to AS asin;
internal_vertices   =   DISTINCT edge_destinations;
num_vertices        =   FOREACH (GROUP internal_vertices ALL) GENERATE COUNT(internal_vertices) AS N;
start_vec           =   FOREACH internal_vertices 
                        GENERATE asin AS i, 1.0 / (double)num_vertices.N AS val;

-- Pig tries to store num_vertices and start_vec, which depends on num_vertices
-- simultaneously, which causes problems. To avoid this, we make a copy.
num_vertices_copy   =   FOREACH num_vertices GENERATE *;

trans_mat           =   TransitionMatrix(edges);
damped_trans_mat    =   MatrixScalarProduct(trans_mat, $DAMPING_FACTOR);

rmf $VECTOR_OUTPUT_PATH;
rmf $MATRIX_OUTPUT_PATH;
rmf $NUM_VERTICES_OUTPUT_PATH;
STORE start_vec INTO '$VECTOR_OUTPUT_PATH' USING PigStorage();
STORE damped_trans_mat INTO '$MATRIX_OUTPUT_PATH' USING PigStorage();
STORE num_vertices_copy INTO '$NUM_VERTICES_OUTPUT_PATH' USING PigStorage();
