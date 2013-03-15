/*
 * Parameters used:
 *
 * NUM_VERTICES_INPUT_PATH
 * EDGES_INPUT_PATH
 * NUM_VERTICES_OUTPUT_PATH
 * TRANS_MAT_OUTPUT_PATH
 */

IMPORT '../macros/matrix.pig';
IMPORT '../macros/graph.pig';

num_verts   =   LOAD '$NUM_VERTICES_INPUT_PATH' USING PigStorage() AS (N: long);
edges       =   LOAD '$EDGES_INPUT_PATH' USING PigStorage() AS (from: bytearray, to: bytearray);

trans_mat   =   TransitionMatrixWithSelfLoops(edges);

rmf $NUM_VERTICES_OUTPUT_PATH;
rmf $TRANS_MAT_OUTPUT_PATH;
STORE num_verts INTO '$NUM_VERTICES_OUTPUT_PATH' USING PigStorage();
STORE trans_mat INTO '$TRANS_MAT_OUTPUT_PATH' USING PigStorage();
