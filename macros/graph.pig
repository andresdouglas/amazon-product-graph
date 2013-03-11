/*
 * Required matrix.pig macros to be imported.
 */

DEFINE TransitionMatrix(edges)
returns trans_mat {
	edges_with_val	=	FOREACH $edges
						-- traditionally, col is from, row is to 
						GENERATE $1 AS row, $0 AS col, 1.0 AS val: double;
	$trans_mat		=	NormalizeMatrix(edges_with_val, 'col');
};

/*
 * WARNING: edges input must not already have self-loops
 */
DEFINE TransitionMatrixWithSelfLoops(edges)
returns trans_mat {
	-- +1 num_inbound_edges to account for the self-loops we will be adding
	-- ignores vertices with no inbound links, since the probability of being at them
	-- would be driven to zero by the iteration anyway
	vertices						=	FOREACH (GROUP $edges BY $1) 
										GENERATE group AS id, COUNT(edges) + 1 AS num_inbound_edges;

	self_loops						= 	FOREACH vertices GENERATE id AS from, id AS to;
	edges_with_self_loops			=	UNION $edges, self_loops;

	edges_with_val					=	FOREACH edges_with_self_loops 
										GENERATE $1 AS row, $0 AS col, 1.0 AS val: double;
	edges_with_vertex_info			=	JOIN edges_with_val BY $1, vertices BY $0;
	$trans_mat						=	FOREACH edges_with_vertex_info 
										GENERATE $0, $1, val / (double)vertices::num_inbound_edges AS prob;
};

DEFINE GoogleMatrix(verts, edges, damping)
returns google_mat {
	edges_with_val		=	FOREACH $edges 
							GENERATE $1 AS row, $0 AS col, 1.0 AS val: double;
	edges_normal		=	NormalizeMatrix(edges_with_val, 'col');
	edges_terms			=	MatrixScalarProduct(edges_normal, $damping);

	verts_copy			=	FOREACH $verts GENERATE *;
	damping_mat_cells	=	CROSS $verts, verts_copy;
	verts_count			=	FOREACH (GROUP $verts ALL) GENERATE COUNT($verts) AS N;
	damping_terms		=	FOREACH damping_mat_cells GENERATE 
								$0 AS row, $1 AS col,
								(1.0 - $damping) / (double)verts_count.N AS val;

	$google_mat			=	MatrixSum(edges_terms, damping_terms);
};

DEFINE MCLIterate(in_mat, inflation_parameter, epsilon)
returns out_mat {
	expansion		=	MatrixSquared($in_mat);
	inflation		=	MatrixElementwisePower(expansion, $inflation_parameter);
	pruning			=	FILTER inflation 
						BY (val > org.apache.pig.piggybank.evaluation.math.POW($epsilon, $inflation_parameter));
	$out_mat		=	NormalizeMatrix(pruning, 'col');
};

/*
 * Requires Enumerate udf alias to be defined,
 * ex. DEFINE Enumerate datafu.pig.bags.Enumerate('1')
 */
DEFINE GetEnumeratedClustersFromMCLResult(mcl_result)
returns enumerated_clusters {
	by_row					=	GROUP $mcl_result BY $0;
	clusters_with_dups		=	FOREACH by_row GENERATE $1.$1 AS cluster;
	clusters_dups_ordered	=	FOREACH clusters_with_dups {
									ordered = ORDER cluster BY $0 ASC;
									GENERATE ordered AS cluster;
								}
	clusters				=	DISTINCT clusters_dups_ordered;

	$enumerated_clusters	=	FOREACH (GROUP clusters ALL)
								GENERATE FLATTEN(Enumerate(clusters)) AS (cluster, i);
};
