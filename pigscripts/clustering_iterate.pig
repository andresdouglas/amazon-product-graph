/*
 * Parameters used:
 *
 * INPUT_PATH
 * ITERATION_OUTPUT_PATH
 * AVG_COL_KURTOSIS_OUTPUT_PATH
 * NUM_VERTICES
 * INFLATION_PARAMETER
 * EPSILON
 */

IMPORT '../macros/matrix.pig';
IMPORT '../macros/graph.pig';

trans_mat   =   LOAD '$INPUT_PATH' USING PigStorage() AS (row, col, val);

iteration   =   MCLIterate(trans_mat, $INFLATION_PARAMETER, $EPSILON);

-- calculate the average kurtosis of each column of the result matrix
-- see iteration comments in clustering_control.py

iteration_with_moments  =   FOREACH iteration 
                            GENERATE *, val*val AS square, val*val*val AS cube, val*val*val*val AS fourth_pow;
columns                 =   GROUP iteration_with_moments BY col;
column_moments          =   FOREACH columns GENERATE
                                group AS col,
                                SUM($1.val) / $NUM_VERTICES AS moment_1,
                                SUM($1.square) / $NUM_VERTICES AS moment_2,
                                SUM($1.cube) / $NUM_VERTICES AS moment_3,
                                SUM($1.fourth_pow) / $NUM_VERTICES AS moment_4;
column_kurtosis         =   FOREACH column_moments GENERATE
                                col,
                                (
                                    (moment_4 - (4 * moment_3 * moment_1)
                                              + (6 * moment_2 * moment_1 * moment_1)
                                              - (3 * moment_1 * moment_1 * moment_1 * moment_1))
                                    / 
                                    (   (moment_2 - (moment_1 * moment_1)) 
                                      * (moment_2 - (moment_1 * moment_1))   )
                                ) AS kurtosis;
avg_col_kurtosis        =   FOREACH (GROUP column_kurtosis ALL) GENERATE AVG($1.kurtosis) AS avg_col_kurtosis;

rmf $ITERATION_OUTPUT_PATH;
rmf $AVG_COL_KURTOSIS_OUTPUT_PATH;
STORE iteration INTO '$ITERATION_OUTPUT_PATH' USING PigStorage();
STORE avg_col_kurtosis INTO '$AVG_COL_KURTOSIS_OUTPUT_PATH' USING PigStorage();
