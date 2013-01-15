from pig_util import outputSchema

# 
# This is where we write python UDFs (User-Defined Functions) that we can call from pig.
# Pig needs to know the schema of the data coming out of the function, 
# which we specify using the @outputSchema decorator.
#
@outputSchema('example_udf:int')
def example_udf(input_str):
    """
    A simple example function that just returns the length of the string passed in.
    """
    return len(input_str) if input_str else None
