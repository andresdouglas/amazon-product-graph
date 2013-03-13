def run_script():
    import os
    from math import log
    from org.apache.pig.scripting import Pig

    # Specify where the data will come from,
    # and where output data will go after each step

    data_stem = "s3n://jpacker-dev/amazon_products/books_graph/"

    num_vertices_input = data_stem + "num_vertices"
    nodes_input = data_stem + "nodes"
    edges_input = data_stem + "edges"

    preprocess_num_vertices_output = data_stem + "clustering/preprocess/num_vertices"
    preprocess_trans_mat_output = data_stem + "clustering/preprocess/trans_mat"
    
    iteration_output_stem = data_stem + "clustering/iteration_"
    avg_col_kurtosis_output_stem = data_stem + "clustering/avg_col_kurtosis_"
    
    postprocess_clusters_output = data_stem + "clustering/clusters"
    postprocess_stats_output = data_stem + "clustering/stats"

    """
    data_stem = "s3n://jpacker-dev/amazon_products/fixtures/"

    nodes_input = data_stem + "cathedral-nodes"
    edges_input = data_stem + "cathedral-edges"

    preprocess_num_vertices_output = data_stem + "cathedral_clustering/preprocess/num_vertices"
    preprocess_trans_mat_output = data_stem + "cathedral_clustering/preprocess/trans_mat"
    
    iteration_output_stem = data_stem + "cathedral_clustering/iteration_"
    avg_col_kurtosis_output_stem = data_stem + "cathedral_clustering/avg_col_kurtosis_"
    
    postprocess_clusters_output = data_stem + "cathedral_clustering/clusters"
    postprocess_stats_output = data_stem + "cathedral_clustering/stats"
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

    # Extract internal vertices count as a number, 
    # which we will pass into each iteration as a parameter

    num_vertices = int(str(preprocess_stats.result("num_verts").iterator().next().get(0)))

    # Parameters for iteration

    iteration = Pig.compileFromFile("../pigscripts/clustering_iterate.pig")
    max_num_iterations = 7                  # stop after this many iterations regardless of whether we've converged
                                            # most graphs should converge after 4-10 iterations
    num_iterations = 0                      # current iteration count

    # see explanation below
    # num_vertices / desired_avg_cluster_size is the desired number of clusters,
    # which can be specified directly if preferred
    desired_avg_cluster_size = 100.0
    convergence_threshold = pow((7.0/3.0), log(num_vertices / desired_avg_cluster_size, 2) + 1)

    last_avg_col_kurtosis = -1000000.0;

    # Iteration step:
    #
    # (1) Apply the Markov Clustering operations: expansion, inflation, pruning, and normalization.
    # (2) Calculate the average kurtosis of each column. This is our convergence metric.
    #
    # The Markov Clustering iteration will converge to a transition matrix where 
    # all of the non-zero values in a column will be the same.
    #
    # Kurtosis is a measure of the "peakedness" of a probability distribution.
    # For a column with a few equal non-zero values and the rest zero, the kurtosis will be high.
    # http://en.wikipedia.org/wiki/Kurtosis
    #
    # So we stop if either 
    #    (a) the average column kurtosis is greater than the convergence threshold,
    #    (b) the average column kurtosis goes down since the last iteration (meaning we are diverging),
    # or (c) we reach the maximum number of iterations allowed
    #
    # The convergence threshold is set to a heuristic given a desired average cluster size;
    # this is an experimental formula however, and may not be appropriate for your data.
    #
    for i in range(1, max_num_iterations + 1):
        iteration_input = preprocess_trans_mat_output if i == 1 else (iteration_output_stem + str(i-1))
        iteration_output = iteration_output_stem + str(i)
        avg_col_kurtosis_output = avg_col_kurtosis_output_stem + str(i)

        iteration_bound = iteration.bind({
            "INPUT_PATH": iteration_input,
            "ITERATION_OUTPUT_PATH": iteration_output,
            "AVG_COL_KURTOSIS_OUTPUT_PATH": avg_col_kurtosis_output,
            "NUM_VERTICES": num_vertices, 
            "INFLATION_PARAMETER": 1.5,     # exponential parameter which determines cluster size if iterated infinitely (higher inflation = smaller clusters)
            "EPSILON": 0.01                 # values in the transition matrix less than epsilon are set to zero to save time and space
        })
        iteration_stats = iteration_bound.runSingle()

        num_iterations += 1
        avg_col_kurtosis = float(str(iteration_stats.result("avg_col_kurtosis").iterator().next().get(0)))
        if num_iterations >= 3 and (avg_col_kurtosis > convergence_threshold or avg_col_kurtosis < last_avg_col_kurtosis):
            break
        else:
            last_avg_col_kurtosis = avg_col_kurtosis

    mcl_result_path = iteration_output_stem + str(num_iterations)

    # Postprocessing step:
    #
    # Interpret the transition matrix outputted by the iterations to find clusters
    # Each row represents a cluster: the column id's of its non-zero elements are its constituents
    #
    # There will be many duplicate clusters (N rows for a cluster of N elements),
    # so we filter those out
    #
    postprocess = Pig.compileFromFile("../pigscripts/clustering_postprocess.pig")
    postprocess_bound = postprocess.bind({
        "NODES_INPUT_PATH": nodes_input,
        "MCL_RESULT_PATH": mcl_result_path, 
        "CLUSTERS_OUTPUT_PATH": postprocess_clusters_output,
        "STATS_OUTPUT_PATH": postprocess_stats_output,
        "MIN_ACCEPTABLE_CLUSTER_SIZE": 0
    })
    postprocess_stats = postprocess_bound.runSingle()

if __name__ == "__main__":
    run_script()
