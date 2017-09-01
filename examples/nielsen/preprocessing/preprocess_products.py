from pyspark import SparkContext, SparkConf
import sys

# prepares products_cleaned_w_brand_code_bu.tsv to be used in the workflow

conf = SparkConf().setAppName("create_div").setMaster("local")
sc = SparkContext(conf=conf)

col_ids = [4, 5, 8, 14]

in_f = sc.textFile(sys.argv[1]) \
    .map(lambda line: tuple(line.split('\t'))) \
    .filter(lambda x: x[15] == 'OZ') \
    .distinct() \
    .map(lambda x: [x[i] for i in col_ids]) \
    .saveAsTextFile(sys.argv[2])