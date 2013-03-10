-- axis: 'row' or 'col'
DEFINE VisualizeMatrix(mat, axis) 
returns vis {
	grouped	=	GROUP $mat BY ('$axis' == 'col'? $1 : $0);
	$vis	=	FOREACH grouped GENERATE $0, ('$axis' == 'col'? $1.($0, $2) : $1.($1, $2));
};

-- axis: 'row' or 'col'
DEFINE NormalizeMatrix(mat, axis)
returns normalized {
	grouped		=	GROUP $mat BY ('$axis' == 'col'? $1 : $0);
	totals		=	FOREACH grouped GENERATE group, SUM($1.$2) AS total;
	with_totals	=	JOIN $mat BY ('$axis' == 'col'? $1 : $0), totals BY group;
	$normalized	=	FOREACH with_totals
					GENERATE $0 AS row, $1 AS col, $2 / (double)totals::total AS val;
};

DEFINE MatrixMultiply(A, B)
returns product {
	joined		=	JOIN $A BY $1, $B BY $0;
	terms		=	FOREACH joined 
					GENERATE $0 AS row, $4 AS col, $2 * $5 AS val;
	by_cell		=	GROUP terms BY (row, col);
	$product	=	FOREACH by_cell GENERATE
						group.$0 AS row,
						group.$1 AS col,
						SUM(terms.val) AS val;
};

DEFINE MatrixSquared(mat)
returns mat_squared {
	copy			=	FOREACH $mat GENERATE *;
	$mat_squared	=	MatrixMultiply($mat, copy);
};

DEFINE ElementwisePower(mat, pow)
returns out {
	$out	=	FOREACH $mat GENERATE $0, $1, org.apache.pig.piggybank.evaluation.math.POW($2, $pow);
};
