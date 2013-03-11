def run_script():
    import os
    from org.apache.pig.scripting import Pig

    nodes_input = "s3n://jpacker-dev/amazon_products/fixtures/kites-nodes"
    edges_input = "s3n://jpacker-dev/amazon_products/fixtures/kites-edges"

    preprocess_output = "s3n://jpacker-dev/amazon_products/fixtures/kites_clustering/preprocess"
    iteration_output_stem = "s3n://jpacker-dev/amazon_products/fixtures/kites_clustering/iteration_"
    max_diff_output_stem = "s3n://jpacker-dev/amazon_products/fixtures/kites_clustering/max_diff_"
    postprocess_clusters_output = "s3n://jpacker-dev/amazon_products/fixtures/kites_clustering/clusters"
    postprocess_stats_output = "s3n://jpacker-dev/amazon_products/fixtures/kites_clustering/stats"

    preprocess = Pig.compileFromFile("../pigscripts/clustering_preprocess.pig")
    bound = preprocess.bind({ 
        "EDGES_INPUT_PATH": edges_input, 
        "OUTPUT_PATH": preprocess_output 
    })
    bound.runSingle()

    iteration = Pig.compileFromFile("../pigscripts/clustering_iterate.pig")
    max_num_iterations = 7
    num_iterations = 0
    convergence_threshold = 0.1;

    for i in range(1, max_num_iterations + 1):
        iteration_input = preprocess_output if i == 1 else (iteration_output_stem + str(i-1))
        iteration_output = iteration_output_stem + str(i)
        max_diff_output = max_diff_output_stem + str(i)

        bound = iteration.bind({
            "INPUT_PATH": iteration_input,
            "ITERATION_OUTPUT_PATH": iteration_output,
            "MAX_DIFF_OUTPUT_PATH": max_diff_output,
            "INFLATION_PARAMETER": 2.0,
            "EPSILON": 0.01
        })
        stats = bound.runSingle()

        num_iterations += 1
        max_diff = float(str(stats.result("max_diff").iterator().next().get(0)))
        if max_diff < convergence_threshold:
            break

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
