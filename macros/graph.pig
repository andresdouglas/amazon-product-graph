/**
 * Requires pig_graph.py to be registered in the calling script
 */

-- edges_input: { t: (v1: int/long, v2: int/long) }
-- vertices should be 1-indexed 
-- All vertices 1 through N vertices must occur at least once as v1

DEFINE AdjacencyMatrix(edges_input)
returns out_mat {
    vertices            =   GROUP $edges_input BY $0;
    all_vertices        =   GROUP vertices ALL;
    vertices_with_count =   FOREACH all_vertices 
                            GENERATE COUNT(vertices), FLATTEN(vertices);
    $out_mat            =   FOREACH vertices_with_count GENERATE FLATTEN(
                                -- $0 = count, $1 = vertex_num, $2 = edges
                                pig_graph.generateAdjacencyMatrix($1, $0, $2, 1)
                            ) AS (row: int, col: int, val: double);
};
