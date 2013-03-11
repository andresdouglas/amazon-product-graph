def run_script():
    import os
    from org.apache.pig.scripting import Pig

    nodes_input = "s3n://jpacker-dev/amazon_products/fixtures/cathedral-nodes"
    edges_input = "s3n://jpacker-dev/amazon_products/fixtures/cathedral-edges"

    preprocess_vector_output = "s3n://jpacker-dev/amazon_products/fixtures/cathedral_pageranks/preprocess/vector"
    preprocess_matrix_output = "s3n://jpacker-dev/amazon_products/fixtures/cathedral_pageranks/preprocess/matrix"
    preprocess_num_vertices_output = "s3n://jpacker-dev/amazon_products/fixtures/cathedral_pageranks/preprocess/num_vertices"
    iteration_output_stem = "s3n://jpacker-dev/amazon_products/fixtures/cathedral_pageranks/iteration_"
    max_diff_output_stem = "s3n://jpacker-dev/amazon_products/fixtures/cathedral_pageranks/max_diff_"
    postprocess_pageranks_output = "s3n://jpacker-dev/amazon_products/fixtures/cathedral_pageranks/pageranks"

    damping_factor = 0.85

    preprocess = Pig.compileFromFile("../pigscripts/pagerank_preprocess.pig")
    preprocess_bound = preprocess.bind({ 
        "NODES_INPUT_PATH": nodes_input, 
        "EDGES_INPUT_PATH": edges_input,
        "VECTOR_OUTPUT_PATH": preprocess_vector_output,
        "MATRIX_OUTPUT_PATH": preprocess_matrix_output,
        "NUM_VERTICES_OUTPUT_PATH": preprocess_num_vertices_output,
        "DAMPING_FACTOR": damping_factor
    })
    preprocess_stats = preprocess_bound.runSingle()

    num_vertices = int(str(preprocess_stats.result("num_vertices_copy").iterator().next().get(0)))

    iteration = Pig.compileFromFile("../pigscripts/pagerank_iterate.pig")
    max_num_iterations = 7
    num_iterations = 0
    convergence_threshold = 0.1

    for i in range(1, max_num_iterations + 1):
        iteration_vector_input = preprocess_vector_output if i == 1 else (iteration_output_stem + str(i-1))
        iteration_matrix_input = preprocess_matrix_output

        iteration_output = iteration_output_stem + str(i)
        max_diff_output = max_diff_output_stem + str(i)

        iteration_bound = iteration.bind({
            "VECTOR_INPUT_PATH": iteration_vector_input,
            "MATRIX_INPUT_PATH": iteration_matrix_input,
            "ITERATION_OUTPUT_PATH": iteration_output,
            "MAX_DIFF_OUTPUT_PATH": max_diff_output,
            "NUM_VERTICES": num_vertices,
            "DAMPING_FACTOR": damping_factor
        })
        iteration_stats = iteration_bound.runSingle()

        num_iterations += 1
        max_diff = float(str(iteration_stats.result("max_diff").iterator().next().get(0)))
        if max_diff < convergence_threshold:
            break

    result_vector = iteration_output_stem + str(num_iterations)

    postprocess = Pig.compileFromFile("../pigscripts/pagerank_postprocess.pig")
    postprocess_bound = postprocess.bind({
        "NODES_INPUT_PATH": nodes_input,
        "RESULT_VECTOR": result_vector,
        "OUTPUT_PATH": postprocess_pageranks_output
    })
    postprocess_bound.runSingle()

if __name__ == "__main__":
    run_script()
