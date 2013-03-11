/*
 * Matrix-scalar operations
 */ 

DEFINE MatrixScalarProduct(mat, mult)
returns out {
    $out    =   FOREACH $mat GENERATE $0 AS row, $1 AS col, $2 * $mult AS val;
};

DEFINE MatrixScalarQuotient(mat, div)
returns out {
    $out    =   FOREACH $mat GENERATE $0 AS row, $1 AS col, $2 / $div AS val;
};

DEFINE MatrixElementwisePower(mat, pow)
returns out {
    $out    =   FOREACH $mat GENERATE 
                    $0 AS row, $1 AS col, 
                    org.apache.pig.piggybank.evaluation.math.POW($2, $pow) AS val;
};

/*
 * Matrix-matrix operations
 */

DEFINE MatrixSum(A, B)
returns out {
    pairs   =   JOIN $A BY ($0, $1) FULL OUTER, $B BY ($0, $1);
    $out    =   FOREACH pairs GENERATE 
                    ($0 is null? $3 : $0) AS row, ($1 is null? $4 : $1) AS col, 
                    ($2 is null? 0 : $2) + ($5 is null? 0 : $5) AS val;
};

DEFINE MatrixDifference(A, B)
returns out {
    pairs   =   JOIN $A BY ($0, $1) FULL OUTER, $B BY ($0, $1);
    $out    =   FOREACH pairs GENERATE 
                    ($0 is null? $3 : $0) AS row, ($1 is null? $4 : $1) AS col, 
                    ($2 is null? 0 : $2) - ($5 is null? 0 : $5) AS val;
};

DEFINE MatrixProduct(A, B)
returns product {
    joined      =   JOIN $A BY $1, $B BY $0;
    terms       =   FOREACH joined GENERATE $0 AS row, $4 AS col, $2 * $5 AS val;
    by_cell     =   GROUP terms BY (row, col);
    $product    =   FOREACH by_cell GENERATE
                        group.$0 AS row, group.$1 AS col,
                        SUM(terms.val) AS val;
};

DEFINE MatrixColumnVectorProduct(M, v)
returns product {
    -- v is one-dimensional, so it should fit into memory
    joined      =   JOIN $M BY $1, $v BY $0 USING 'replicated';
    terms       =   FOREACH joined GENERATE $0 AS i, $2 * $4 AS val;
    by_cell     =   GROUP terms BY i;
    $product    =   FOREACH by_cell 
                    GENERATE group AS i, SUM(terms.val) AS val;
};

DEFINE MatrixSquared(mat)
returns mat_squared {
    copy            =   FOREACH $mat GENERATE *;
    $mat_squared    =   MatrixProduct($mat, copy);
};

/*
 * Matrix unary operations
 */

-- TODO: determinant
-- TODO: inverse

/*
 * Other operations
 */

-- axis: 'row' or 'col'
DEFINE NormalizeMatrix(mat, axis)
returns normalized {
    grouped         =   GROUP $mat BY ('$axis' == 'col'? $1 : $0);
    totals          =   FOREACH grouped GENERATE group, SUM($1.$2) AS total;
    with_totals     =   JOIN $mat BY ('$axis' == 'col'? $1 : $0), totals BY group;
    $normalized     =   FOREACH with_totals
                        GENERATE $0 AS row, $1 AS col, $2 / (double)totals::total AS val;
};

-- axis: 'row' or 'col'
DEFINE VisualizeMatrix(mat, axis) 
returns vis {
    grouped =   GROUP $mat BY ('$axis' == 'col'? $1 : $0);
    $vis    =   FOREACH grouped GENERATE $0, ('$axis' == 'col'? $1.($0, $2) : $1.($1, $2));
};
