/*
 * Parameters used:
 *
 * INPUT_PATH
 * ITERATION_OUTPUT_PATH
 * NUM_VERTICES
 * INFLATION_PARAMETER
 * EPSILON
 */

IMPORT '../macros/matrix.pig';
IMPORT '../macros/graph.pig';

trans_mat       =   LOAD '$INPUT_PATH' USING PigStorage() AS (row, col, val);
iteration       =   MCLIterate(trans_mat, $INFLATION_PARAMETER, $EPSILON);

rmf $ITERATION_OUTPUT_PATH;
STORE iteration INTO '$ITERATION_OUTPUT_PATH' USING PigStorage();
