/*
 * INPUT_PATH
 * ITERATION_OUTPUT_PATH
 * AVG_COL_STD_DEV_OUTPUT_PATH
 * INFLATION_PARAMETER
 * EPSILON
 */

IMPORT '../macros/matrix.pig';
IMPORT '../macros/graph.pig';

trans_mat   =   LOAD '$INPUT_PATH' USING PigStorage() AS (row, col, val);

iteration   =   MCLIterate(trans_mat, $INFLATION_PARAMETER, $EPSILON);

-- NOTE: column std dev here refers to the standard deviation
--       of all NON-ZERO values in the column
--       iteration will converge to a column idempotent matrix
--       (all non-zero values in the column will be the same)

iteration_with_squares  =   FOREACH iteration GENERATE *, val * val AS square;
columns                 =   GROUP iteration_with_squares BY col;
column_means            =   FOREACH columns 
                            GENERATE group AS col, AVG($1.val) AS mean, AVG($1.square) AS mean_of_squares;
column_std_devs         =   FOREACH column_means GENERATE col, SQRT(mean_of_squares - (mean * mean)) AS std_dev;
avg_col_std_dev         =   FOREACH (GROUP column_std_devs ALL) GENERATE AVG($1.std_dev);

rmf $ITERATION_OUTPUT_PATH;
rmf $AVG_COL_STD_DEV_OUTPUT_PATH;
STORE iteration INTO '$ITERATION_OUTPUT_PATH' USING PigStorage();
STORE avg_col_std_dev INTO '$AVG_COL_STD_DEV_OUTPUT_PATH' USING PigStorage();
