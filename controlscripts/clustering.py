# An implementation of the Markov Clustering (MCL) algorithm
# See http://www.cs.ucsb.edu/~xyan/classes/CS595D-2009winter/MCL_Presentation2.pdf
#
# The advantage of MCL is that it clusters purely by the structure of the graph
# and does not require feature vectors or a distance metric. It also converges quickly.
#
# The disadvantage is that cluster size is configured by an exponential parameter
# (usually 1.5-2.0) and there is no easy way to tell what size clusters a given
# value of the parameter will give for your data other than guess-and-checking.
#
# This implementation uses unweighted edges, but all that needs to be done to
# add edge weights is to modify the TransitionMatrix macros in graph.pig
# to initialize edges to a weight value instead of just 1. The algorithm
# normalizes the values, so you do not need to do that beforehand.
#
def run_script():
    import os
    from org.apache.pig.scripting import Pig

    # Specify where the data will come from,
    # and where output data will go after each step

    data_stem = "s3n://jpacker-dev/amazon_products/books_graph/"
    num_vertices_input = data_stem + "num_vertices"
    nodes_input = data_stem + "nodes"
    edges_input = data_stem + "edges"

    output_stem = data_stem + "clustering/"
    preprocess_num_vertices_output = output_stem + "preprocess/num_vertices"
    preprocess_trans_mat_output = output_stem + "preprocess/trans_mat"
    iteration_trans_mat_output_stem = output_stem + "iteration/trans_mat_"
    postprocess_clusters_output = output_stem + "postprocess/clusters"
    postprocess_stats_output = output_stem + "postprocess/stats"

    """
    data_stem = "../fake-fixtures/"
    num_vertices_input = data_stem + "cathedral-num-vertices"
    nodes_input = data_stem + "cathedral-nodes"
    edges_input = data_stem + "cathedral-edges"

    output_stem = data_stem + "cathedral_clustering/"
    preprocess_num_vertices_output = output_stem + "preprocess/num_vertices"
    preprocess_trans_mat_output = output_stem + "preprocess/trans_mat"
    iteration_trans_mat_output_stem = output_stem + "iteration/trans_mat_"
    postprocess_clusters_output = output_stem + "postprocess/clusters"
    postprocess_stats_output = output_stem + "postprocess/stats"
    """

    # Preprocessing step:
    #
    # (1) Generate a transition matrix from the internal edges
    # (2) Copy precomputed count of # vertices
    #     No computation is being done here; this just lets us use Pig to access the data
    #     instead of configuring S3 access manually with boto
    #
    preprocess = Pig.compileFromFile("../pigscripts/clustering_preprocess.pig")
    preprocess_bound = preprocess.bind({ 
        "NUM_VERTICES_INPUT_PATH": num_vertices_input, 
        "EDGES_INPUT_PATH": edges_input, 
        "NUM_VERTICES_OUTPUT_PATH": preprocess_num_vertices_output,
        "TRANS_MAT_OUTPUT_PATH": preprocess_trans_mat_output
    })
    preprocess_stats = preprocess_bound.runSingle()

    # Extract the number of vertices, which we will pass into each iteration as a parameter
    num_vertices = long(str(preprocess_stats.result("num_verts").iterator().next().get(0)))
    
    # Extract the number of edges (including inserted self-loops)
    # We will use this in our convergence metric
    initial_num_edges = long(str(preprocess_stats.getNumberRecords(preprocess_trans_mat_output)))
    
    # Iteration step applying the Markov Clustering operations:
    #
    # (1) Expansion: square the transition matrix ~= take a step in a random walk
    # (2) Inflation: take an elementwise power of the matrix ~= strengthen strong connections, weaken weak ones'
    # (3) Pruning: set small matrix values to zero (since the matrix impl is sparse, this greatly speeds things up)
    # (4) Normalization: renormalize the matrix columnwise to keep it a valid transition matrix
    #
    # I tested several mathematically sensible convergence metrics 
    # (max of max residual for each col, avg of max residual for each col, col kurtosis)
    # but none worked very well. So I'm currently just breaking when the number of edges
    # in an iteration's transition matrix is less than the number of edges in 
    # the initial transition matrix times a constant multiple, which seems to indicate
    # that things are settling down.
    #
    # The algorithm has two parameters:
    # (1) The inflation parameter is an exponential factor which determines the cluster size. higher inflation => smaller clusters
    # (2) Epsilon is a minimum threshold for values in the transition matrix; anything smaller will be pruned (set to zero)
    #     I am not sure how high epsilon can safely be set without significantly degrading the quality of the algorithm
    #     If you run in to performance problems though, raising epsilon will dramatically reduce execution time
    #
    iteration = Pig.compileFromFile("../pigscripts/clustering_iterate.pig")
    max_num_iterations = 7  # most graphs should converge after 4-10 iterations
    num_iterations = 0

    for i in range(1, max_num_iterations + 1):
        iteration_input = preprocess_trans_mat_output if i == 1 else (iteration_trans_mat_output_stem + str(i-1))
        iteration_output = iteration_trans_mat_output_stem + str(i)

        iteration_bound = iteration.bind({
            "INPUT_PATH": iteration_input,
            "ITERATION_OUTPUT_PATH": iteration_output,
            "NUM_VERTICES": num_vertices, 
            "INFLATION_PARAMETER": 1.5,
            "EPSILON": 0.01
        })
        iteration_stats = iteration_bound.runSingle()

        num_iterations += 1
        num_edges = long(str(iteration_stats.getNumberRecords(iteration_output)))
        if num_iterations >= 3 and num_edges < (initial_num_edges * 1.05):
            break

    # Postprocessing step:
    #
    # Interpret the transition matrix outputted by the iterations to find clusters.
    # Each row represents a cluster: the column id's of its non-zero elements are its constituents.
    #
    # There will be many duplicate clusters (N rows for a cluster of N elements),
    # so we filter those out. We also filter out very small clusters.
    #
    mcl_result_path = iteration_trans_mat_output_stem + str(num_iterations)
    postprocess = Pig.compileFromFile("../pigscripts/clustering_postprocess.pig")
    postprocess_bound = postprocess.bind({
        "NODES_INPUT_PATH": nodes_input,
        "MCL_RESULT_PATH": mcl_result_path, 
        "CLUSTERS_OUTPUT_PATH": postprocess_clusters_output,
        "STATS_OUTPUT_PATH": postprocess_stats_output,
        "MIN_ACCEPTABLE_CLUSTER_SIZE": 3
    })
    postprocess_stats = postprocess_bound.runSingle()

if __name__ == "__main__":
    run_script()
