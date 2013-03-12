# NOTE: col std dev here refers to the standard deviation
#       of all NON-ZERO values in a column.
#       iteration will converge to a column idempotent matrix
#       (all non-zero values in the column will be the same)

def run_script():
    import os
    from org.apache.pig.scripting import Pig

    nodes_input = "s3n://jpacker-dev/amazon_products/fixtures/cathedral-nodes"
    edges_input = "s3n://jpacker-dev/amazon_products/fixtures/cathedral-edges"

    preprocess_output = "s3n://jpacker-dev/amazon_products/fixtures/cathedral_clustering/preprocess"
    iteration_output_stem = "s3n://jpacker-dev/amazon_products/fixtures/cathedral_clustering/iteration_"
    avg_col_std_dev_output_stem = "s3n://jpacker-dev/amazon_products/fixtures/cathedral_clustering/avg_col_std_dev_"
    postprocess_clusters_output = "s3n://jpacker-dev/amazon_products/fixtures/cathedral_clustering/clusters"
    postprocess_stats_output = "s3n://jpacker-dev/amazon_products/fixtures/cathedral_clustering/stats"

    preprocess = Pig.compileFromFile("../pigscripts/clustering_preprocess.pig")
    bound = preprocess.bind({ 
        "EDGES_INPUT_PATH": edges_input, 
        "OUTPUT_PATH": preprocess_output 
    })
    bound.runSingle()

    iteration = Pig.compileFromFile("../pigscripts/clustering_iterate.pig")
    max_num_iterations = 7
    num_iterations = 0
    convergence_threshold = 0.02;   # average col std dev for convergence
    last_avg_col_std_dev = 0.5;     # maximum col std dev for a column stochastic matrix

    for i in range(1, max_num_iterations + 1):
        iteration_input = preprocess_output if i == 1 else (iteration_output_stem + str(i-1))
        iteration_output = iteration_output_stem + str(i)
        avg_col_std_dev_output = avg_col_std_dev_output_stem + str(i)

        bound = iteration.bind({
            "INPUT_PATH": iteration_input,
            "ITERATION_OUTPUT_PATH": iteration_output,
            "AVG_COL_STD_DEV_OUTPUT_PATH": avg_col_std_dev_output,
            "INFLATION_PARAMETER": 2.0,
            "EPSILON": 0.01
        })
        stats = bound.runSingle()

        num_iterations += 1
        avg_col_std_dev = float(str(stats.result("avg_col_std_dev").iterator().next().get(0)))
        if num_iterations >= 3 and (avg_col_std_dev < convergence_threshold or avg_col_std_dev > last_avg_col_std_dev):
            break
        else:
            last_avg_col_std_dev = avg_col_std_dev

    mcl_result_path = iteration_output_stem + str(num_iterations)

    postprocess = Pig.compileFromFile("../pigscripts/clustering_postprocess.pig")
    bound = postprocess.bind({
        "NODES_INPUT_PATH": nodes_input,
        "MCL_RESULT_PATH": mcl_result_path, 
        "CLUSTERS_OUTPUT_PATH": postprocess_clusters_output,
        "STATS_OUTPUT_PATH": postprocess_stats_output,
        "MIN_ACCEPTABLE_CLUSTER_SIZE": 0
    })
    bound.runSingle()

if __name__ == "__main__":
    run_script()
