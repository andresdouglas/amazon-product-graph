%default INPUT_PATH '/home/jpacker/code/test/amazon_products/clusters'

REGISTER 's3n://jpacker-dev/jar/datafu-0.0.9.jar';
DEFINE Quantiles datafu.pig.stats.StreamingQuantile('5'); -- min, 25%, 50%, 75%, max

clusters    =   LOAD '$INPUT_PATH' USING PigStorage() 
                AS (i: bytearray, size: int, cluster: {t: (asin: bytearray, title: bytearray)});
sizes       =   FOREACH clusters GENERATE size;
quantiles   =   FOREACH (GROUP sizes ALL) GENERATE Quantiles($1);

STORE quantiles INTO '/home/jpacker/Downloads/quantiles' USING PigStorage();
