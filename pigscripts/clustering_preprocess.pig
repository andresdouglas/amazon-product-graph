/*
 * EDGES_INPUT_PATH
 * OUTPUT_PATH
 */

IMPORT '../macros/matrix.pig';
IMPORT '../macros/graph.pig';

edges       =   LOAD '$EDGES_INPUT_PATH' USING PigStorage() 
                AS (from: chararray, to: chararray);

trans_mat   =   TransitionMatrixWithSelfLoops(edges);

rmf $OUTPUT_PATH;
STORE trans_mat INTO '$OUTPUT_PATH' USING PigStorage();
