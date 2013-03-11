/*
 * INPUT_PATH
 * ITERATION_OUTPUT_PATH
 * MAX_DIFF_OUTPUT_PATH
 */

IMPORT '../macros/matrix.pig';
IMPORT '../macros/graph.pig';

trans_mat   =   LOAD '$INPUT_PATH' USING PigStorage() AS (row, col, val);

iteration   =   MCLIterate(trans_mat, $INFLATION_PARAMETER, $EPSILON);

diff        =   MatrixDifference(iteration, trans_mat);
abs_diffs   =   FOREACH diff GENERATE ABS(val);
max_diff    =   FOREACH (GROUP abs_diffs ALL) GENERATE MAX($1);

rmf $ITERATION_OUTPUT_PATH;
rmf $MAX_DIFF_OUTPUT_PATH;
STORE iteration INTO '$ITERATION_OUTPUT_PATH' USING PigStorage();
STORE max_diff INTO '$MAX_DIFF_OUTPUT_PATH' USING PigStorage();
