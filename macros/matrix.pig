/*
 * mat: { row, col, val }
 * by_row_or_col: 'row' or 'col'
 * ==>
 * vis: { row/col: {t: (col/row, val)} }
 */
DEFINE VisualizeMatrix(mat, by_row_or_col) 
returns vis {
	grouped	=	GROUP $mat BY ('$by_row_or_col' == 'col'? $1 : $0);
	$vis	=	FOREACH grouped GENERATE $0, ('$by_row_or_col' == 'col'? $1.($0, $2) : $1.($1, $2));
};

/*
 * mat: { row, col, val }
 * by_row_or_col: 'row' or 'col'
 * ==>
 * normalized: { row, col, val }
 */
DEFINE NormalizeMatrix(mat, by_row_or_col)
returns normalized {
	grouped		=	GROUP $mat BY ('$by_row_or_col' == 'col'? $1 : $0);
	totals		=	FOREACH grouped GENERATE group, SUM($1.$2) AS total;
	with_totals	=	JOIN $mat BY ('$by_row_or_col' == 'col'? $1 : $0), totals BY group;
	$normalized	=	FOREACH with_totals
					GENERATE $0 AS row, $1 AS col, $2 / (double)totals::total AS val;
};

/*
 * A: { row, col, val }
 * B: { row, col, val }
 * null_value: 0 for integer matrices, 0.0 for decimal matrices
 * ==>
 * product: { row, col, val }
 */
DEFINE MatrixMultiply(A, B, null_value)
returns product {
	joined		=	JOIN $A BY $1 FULL OUTER, $B BY $0;
	terms		=	FOREACH joined GENERATE
						$0 AS row,
						$4 AS col,
						($2 is null? $null_value : $2) * ($5 is null? $null_value : $5) AS val;
	by_cell		=	GROUP terms BY (row, col);
	$product	=	FOREACH by_cell GENERATE
						group.$0 AS row,
						group.$1 AS col,
						SUM(terms.val) AS val;
};

/*
 * mat: { row, col, val }
 * null_value: 0 for integer matrices, 0.0 for decimal matrices
 * ==>
 * mat_squared: { row, col, val }
 */
DEFINE MatrixSquared(mat, null_value)
returns mat_squared {
	copy			=	FOREACH $mat GENERATE *;
	$mat_squared	=	MatrixMultiply($mat, copy, $null_value);
};

/*
 * mat: { row, col, val }
 * pow: int/long/float/double
 * ==>
 * out: { row, col, val }
 */
 DEFINE ElementwisePower(mat, pow)
 returns out {
 	$out	=	FOREACH $mat GENERATE $0, $1, org.apache.pig.piggybank.evaluation.math.POW($2, $pow);
 };