from pig_util import outputSchema

# Takes vec: { t: (row: int/long, col: int/long, val: float/double) }
#
# vec should be a row/column of a matrix
# Ex. FOREACH mat_by_row GENERATE pig_matrix.normalizeMatrix(row)
#
@outputSchema("vec: { t: (row: int, col: int, val: double) }")
def normalizeMatrix(vec):
	magnitude = sum([cell[2]*cell[2] for cell in vec])
	return [(cell[0], cell[1], cell[2] / magnitude) for cell in vec]

# Takes rowNum: int/long,
#       rowCells: { t: (row: int/long, col: int/long, val: float/double) },
#       colNum: int/long
#       colCells: { t: (row: int/long, col: int/long, val: float/double) }
#
@outputSchema("cell: (row: int, col: int, val: double)")
def cellDotProduct(rowNum, rowCells, colNum, colCells):
	return (rowNum, colNum, sum([rowCells[i][2]*colCells[i][2] for i in range(len(rowCells))]))
