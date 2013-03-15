/*
 * Parameters used:
 *
 * INPUT_PATH
 * ITERATION_OUTPUT_PATH
 * MAX_RESIDUAL_OUTPUT_PATH
 * NUM_VERTICES
 * INFLATION_PARAMETER
 * EPSILON
 */

IMPORT '../macros/matrix.pig';
IMPORT '../macros/graph.pig';

trans_mat       =   LOAD '$INPUT_PATH' USING PigStorage() AS (row, col, val);
iteration       =   MCLIterate(trans_mat, $INFLATION_PARAMETER, $EPSILON);

squares         =   FOREACH iteration GENERATE *, val*val AS square;
residuals       =   FOREACH (GROUP squares BY row) GENERATE (MAX($1.val)-SUM($1.square)) AS residual: double;
max_residual    =   FOREACH (GROUP residuals ALL) GENERATE MAX($1);

rmf $ITERATION_OUTPUT_PATH;
rmf $MAX_RESIDUAL_OUTPUT_PATH;
STORE iteration INTO '$ITERATION_OUTPUT_PATH' USING PigStorage();
STORE max_residual INTO '$MAX_RESIDUAL_OUTPUT_PATH' USING PigStorage();
