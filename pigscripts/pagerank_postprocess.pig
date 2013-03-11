IMPORT '../macros/matrix.pig';
IMPORT '../macros/graph.pig';

nodes               =   LOAD '$NODES_INPUT_PATH' USING PigStorage() 
                        AS (asin: chararray, title: chararray);
                        --    AS (asin: chararray, title: chararray, maker: chararray, 
                        --        price: float, rating: float, raters: int);

result_vector       =   LOAD '$RESULT_VECTOR' USING PigStorage()
                        AS (i: chararray, val: double);

pageranks           =   FOREACH result_vector GENERATE i AS asin, val AS pagerank;
pagerank_sum        =   FOREACH (GROUP pageranks ALL) 
                        GENERATE SUM(pageranks.pagerank) AS sum;

-- renormalize to mitigate accumulated numerical error
renormalized        =   FOREACH pageranks 
                        GENERATE asin, pagerank / pagerank_sum.sum AS pagerank;
with_titles         =   JOIN renormalized BY asin, nodes BY asin;
pageranks_out       =   FOREACH with_titles GENERATE $0, $1, nodes::title;

rmf $OUTPUT_PATH;
STORE pageranks_out INTO '$OUTPUT_PATH' USING PigStorage();
