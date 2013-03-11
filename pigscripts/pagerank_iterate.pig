/*
 * VECTOR_INPUT_PATH
 * MATRIX_INPUT_PATH
 * ITERATION_OUTPUT_PATH
 * MAX_DIFF_OUTPUT_PATH
 */

IMPORT '../macros/matrix.pig';
IMPORT '../macros/graph.pig';

vector      =   LOAD '$VECTOR_INPUT_PATH' USING PigStorage() AS (i: chararray, val: double);
matrix      =   LOAD '$MATRIX_INPUT_PATH' USING PigStorage() AS (row: chararray, col: chararray, val: double);
iteration   =   PagerankIterate(vector, matrix, $NUM_VERTICES, $DAMPING_FACTOR);

vec_join    =   JOIN iteration BY i, vector BY i;
vec_diff    =   FOREACH vec_join GENERATE ABS(iteration::val - vector::val);
max_diff    =   FOREACH (GROUP vec_diff ALL) GENERATE MAX($1);

rmf $ITERATION_OUTPUT_PATH;
rmf $MAX_DIFF_OUTPUT_PATH;
STORE iteration INTO '$ITERATION_OUTPUT_PATH' USING PigStorage();
STORE max_diff INTO '$MAX_DIFF_OUTPUT_PATH' USING PigStorage();
