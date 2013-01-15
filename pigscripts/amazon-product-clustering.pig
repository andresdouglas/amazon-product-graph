/**
 * amazon-product-clustering
 *
 * Required parameters:
 *
 * - INPUT_PATH Input path for script data (e.g. s3n://hawk-example-data/tutorial/excite.log.bz2)
 * - OUTPUT_PATH Output path for script data (e.g. s3n://my-output-bucket/amazon-product-clustering)
 */

/**
 * User-Defined Functions (UDFs)
 */
REGISTER '../udfs/python/amazon-product-clustering.py' USING streaming_python AS amazon-product-clustering;

-- This is an example of loading up input data
my_input_data = LOAD '$INPUT_PATH' 
               USING PigStorage('\t') 
                  AS (field0:chararray, field1:chararray, field2:chararray);

-- This is an example pig operation
filtered = FILTER my_input_data
               BY field0 IS NOT NULL;

-- This is an example call to a python user-defined function
with_udf_output = FOREACH filtered 
                 GENERATE field0..field2, 
                          amazon-product-clustering.example_udf(field0) AS example_udf_field;

-- remove any existing data
rmf $OUTPUT_PATH;

-- store the results
STORE with_udf_output 
 INTO '$OUTPUT_PATH' 
USING PigStorage('\t');
