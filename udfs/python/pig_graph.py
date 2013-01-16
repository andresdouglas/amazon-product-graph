from pig_util import outputSchema

# Takes vertexNum: int/long, 
#       count: int/long, 
#       edges: { t: (v1: int/long, v2: int/long) }, 
#       addSelfLoops: int, boolean, whatever
#
# All edges (i, i) are added if addSelfLoops evaluates to true
#
@outputSchema("row: { t: (row: int, col: int, val: double) }")
def generateAdjacencyMatrix(vertexNum, count, edges, addSelfLoops):
	cells = {}

	for i in range(1, count+1):
		if addSelfLoops and i == vertexNum:
			cells[(vertexNum, i)] = 1.0
		else:
			cells[(vertexNum, i)] = 0.0

	for edge in edges:
		cells[(edge[0], edge[1])] = 1.0

	return [(key[0], key[1], val) for key, val in cells.iteritems()]
