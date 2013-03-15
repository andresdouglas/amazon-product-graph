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

    """
    data_stem = "s3n://jpacker-dev/amazon_products/books_graph/"

    num_vertices_input = data_stem + "num_vertices"
    nodes_input = data_stem + "nodes"
    edges_input = data_stem + "edges"

    preprocess_num_vertices_output = data_stem + "clustering/preprocess/num_vertices"
    preprocess_trans_mat_output = data_stem + "clustering/preprocess/trans_mat"
    iteration_trans_mat_output_stem = data_stem + "clustering/iteration/trans_mat_"
    iteration_max_residual_output_stem = data_stem + "clustering/iteration/max_residual_"
    postprocess_clusters_output = data_stem + "clustering/postprocess/clusters"
    postprocess_stats_output = data_stem + "clustering/postprocess/stats"
    """

    data_stem = "s3n://jpacker-dev/amazon_products/fixtures/"

    num_vertices_input = data_stem + "kites-num-vertices"
    nodes_input = data_stem + "kites-nodes"
    edges_input = data_stem + "kites-edges"

    preprocess_num_vertices_output = data_stem + "kites_clustering/preprocess/num_vertices"
    preprocess_trans_mat_output = data_stem + "kites_clustering/preprocess/trans_mat"
    iteration_trans_mat_output_stem = data_stem + "kites_clustering/iteration/trans_mat_"
    iteration_max_residual_output_stem = data_stem + "kites_clustering/iteration/max_residual_"
    postprocess_clusters_output = data_stem + "kites_clustering/postprocess/clusters"
    postprocess_stats_output = data_stem + "kites_clustering/postprocess/stats"
    

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

    # Iteration step:
    #
    # (1) Apply the Markov Clustering operations: expansion, inflation, pruning, and normalization.
    # (2) Find the maximum column residual = max([max(col) - sum_of_squares(col) for col in matrix])
    #    
    # The iteration will produce a new transition matrix at each step;
    # eventually, these matrices will converge to a "column idempotent" matrix,
    # meaning that each value in the column will be the same. Subtracting
    # the sum of the squares of the column values from the maximum column values
    # is a metric to describe how idempotent a column is. When this passes
    # below a specified threshold (or we reach the maximum permitted number of iterations)
    # we stop iterating.
    #
    # The algorithm has two parameters:
    # (1) The inflation parameter is an exponential factor which determines the cluster size. higher inflation => smaller clusters
    # (2) Epsilon is a minimum threshold for values in the transition matrix; anything smaller will be pruned (set to zero)
    #     I am not sure how high epsilon can safely be set without significantly degrading the quality of the algorithm
    #     If you run in to performance problems though, raising epsilon will dramatically reduce execution time
    #
    iteration = Pig.compileFromFile("../pigscripts/clustering_iterate.pig")
    max_num_iterations = 5                  # most graphs should converge after 4-10 iterations
    num_iterations = 0                      # current iteration count
    convergence_threshold = 0.01            # stop iterating if max([max(col) - sum_of_squares(col) for col in matrix]) < this
    last_max_residual = 1.0                 # if the max residual starts increasing instead of decreasing,
                                            # then the system is diverging and we break
                                            # (not sure if this is mathematically possible, but it doesn't hurt to be safe)
    for i in range(1, max_num_iterations + 1):
        iteration_input = preprocess_trans_mat_output if i == 1 else (iteration_trans_mat_output_stem + str(i-1))
        iteration_output = iteration_trans_mat_output_stem + str(i)
        max_residual_output = iteration_max_residual_output_stem + str(i)

        iteration_bound = iteration.bind({
            "INPUT_PATH": iteration_input,
            "ITERATION_OUTPUT_PATH": iteration_output,
            "MAX_RESIDUAL_OUTPUT_PATH": max_residual_output,
            "NUM_VERTICES": num_vertices, 
            "INFLATION_PARAMETER": 1.5,
            "EPSILON": 0.01
        })
        iteration_stats = iteration_bound.runSingle()

        num_iterations += 1
        max_residual = float(str(iteration_stats.result("max_residual").iterator().next().get(0)))
        if num_iterations >= 5 and (max_residual <= convergence_threshold or max_residual > last_max_residual):
            break
        else:
            last_max_residual = max_residual

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
