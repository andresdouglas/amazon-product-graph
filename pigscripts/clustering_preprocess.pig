/*
 * EDGES_INPUT_PATH
 * OUTPUT_PATH
 */

IMPORT '../macros/matrix.pig';
IMPORT '../macros/graph.pig';

edges       =   LOAD '$EDGES_INPUT_PATH' USING PigStorage() 
                AS (from: chararray, to: chararray);

edge_destinations   =   GROUP edges BY to;
internal_vertices   =   FILTER edge_destinations BY (group is not null);
internal_edges      =   FOREACH internal_vertices GENERATE FLATTEN(edges);

trans_mat           =   TransitionMatrixWithSelfLoops(internal_edges);

rmf $OUTPUT_PATH;
STORE trans_mat INTO '$OUTPUT_PATH' USING PigStorage();
