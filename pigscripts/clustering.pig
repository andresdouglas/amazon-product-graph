%default NODES_INPUT_PATH 's3n://jpacker-dev/amazon_products/fixtures/kites-nodes'
-- %default NODES_INPUT_PATH 's3n://jpacker-dev/amazon_products/fixtures/cathedral-nodes'
-- %default NODES_INPUT_PATH 's3n://jpacker-dev/amazon_products/books_graph/nodes'

%default EDGES_INPUT_PATH 's3n://jpacker-dev/amazon_products/fixtures/kites-edges'
-- %default EDGES_INPUT_PATH 's3n://jpacker-dev/amazon_products/fixtures/cathedral-edges'
-- %default EDGES_INPUT_PATH 's3n://jpacker-dev/amazon_products/books_graph/edges'

%default OUTPUT_PATH 's3n://jpacker-dev/amazon_products/fixtures/kites_clustering'
-- %default OUTPUT_PATH 's3n://jpacker-dev/amazon_products/fixtures/cathedral_clustering'
-- %default OUTPUT_PATH 's3n://jpacker-dev/amazon_products/books_graph/clustering'

%default INFLATION_PARAMETER '2.0'

IMPORT '../macros/matrix.pig';
IMPORT '../macros/graph.pig';

nodes	=	LOAD '$NODES_INPUT_PATH' USING PigStorage() 
			AS (asin: chararray, title: chararray, maker: chararray, price: float, rating: float, raters: int);
edges	=	LOAD '$EDGES_INPUT_PATH' USING PigStorage() 
			AS (from: chararray, to: chararray);

trans_mat 				= 	NormalizedTransitionMatrixWithSelfLoops(edges, 'col');
expansion_1				=	MatrixSquared(trans_mat, 0.0);
inflated_1				=	ElementwisePower(expansion_1, $INFLATION_PARAMETER);
normalized_1			=	NormalizeMatrix(inflated_1, 'col');
vis						=	VisualizeMatrix(normalized_1, 'col');
