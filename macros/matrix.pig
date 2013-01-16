/**
 * Requires pig_matrix.py to be registered in the calling script
 */

-- in_mat: { t: (row: int/long, col: int/long, val: float/double) }
-- group_field: row or col
DEFINE NormalizeMatrix(in_mat, group_field)
returns out_mat {
    grouped_mat =   GROUP $in_mat BY $group_field;
    $out_mat    =   FOREACH grouped_mat GENERATE FLATTEN(
                        pig_matrix.normalizeMatrix($in_mat)
                    );
};

-- A: { t: (row: int/long, col: int/long, val: float/double) }
-- B: { t: (row: int/long, col: int/long, val: float/double) }
DEFINE MatrixMultiply(A, B)
returns mult {
    a_by_row        =   GROUP $A BY row;
    b_by_col        =   GROUP $B BY col;
    row_col_pairs   =   CROSS a_by_row, b_by_col;
    $mult           =   FOREACH row_col_pairs
                        GENERATE pig_matrix.cellDotProduct($0, $1, $2, $3);
};
