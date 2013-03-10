/*
 * Required matrix.pig macros to be imported.
 */

/*
 * edges: { from, to }
 * by_row_or_col: 'row' or 'col'
 * ==>
 * trans_mat: { row, col, val }
 */
DEFINE NormalizedTransitionMatrix(edges, by_row_or_col)
returns trans_mat {
	edges_with_val					=	FOREACH $edges GENERATE *, 1.0 AS val: double;
	$trans_mat						=	NormalizeMatrix(edges_with_val, '$by_row_or_col');
};

/*
 * edges: { from, to }
 * WARNING: edges input must not have self-loops
 * by_row_or_col: 'row' or 'col'
 * ==>
 * trans_mat: { row, col, val }
 */
DEFINE NormalizedTransitionMatrixWithSelfLoops(edges, by_row_or_col)
returns trans_mat {
	-- +1 to account for the self-loops we will be adding
	vertices						=	FOREACH (GROUP $edges BY ('$by_row_or_col' == 'col'? $1 : $0)) 
										GENERATE group AS id, COUNT(edges) + 1 AS num_edges;

	self_loops						= 	FOREACH vertices GENERATE id AS from, id AS to;
	edges_with_self_loops_and_dups	=	UNION $edges, self_loops;
	edges_with_self_loops			= 	DISTINCT edges_with_self_loops_and_dups;

	edges_with_val					=	FOREACH edges_with_self_loops GENERATE *, 1.0 AS val: double;
	edges_with_vertex_info			=	JOIN edges_with_val BY ('$by_row_or_col' == 'col'? $1 : $0), vertices BY $0;
	$trans_mat						=	FOREACH edges_with_vertex_info 
										GENERATE $0, $1, val / (double)vertices::num_edges AS prob;
};
