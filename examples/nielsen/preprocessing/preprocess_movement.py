import findspark

findspark.init('/usr/local/opt/apache-spark/libexec')

from pyspark import SparkContext, SparkConf
import sys, os, numpy


'''
Concatenates data from 2008 movement files and merges it
with retailer & store location info from a stores file.

Input args:
1. path to movement files directory
2. path to stores file
3. output file path
'''

conf = SparkConf().setAppName("preprocess").setMaster("local")
sc = SparkContext(conf=conf)

movement_files_dir = sys.argv[1]

# load stores data, remove header and rows where
# retailer_code is blank, select out relevant columns
col_ids = [0, 3, 5]
stores = sc.textFile(sys.argv[2]) \
    .map(lambda x: x.split('\t')) \
    .filter(lambda x: x[0] != 'store_code_uc') \
    .filter(lambda x: x[3].strip() != '') \
    .map(lambda x: [x[i] for i in col_ids])

concat = ''
# concatenate movement files
for subdir, dirs, files in os.walk(movement_files_dir):
    for file in files:
        if file[0] != '.':
            print("Processing file: {0}".format(file))
            in_f = sc.textFile(movement_files_dir + file) \
                .map(lambda x: x.split('\t')) \
                .filter(lambda x: x[0] != 'store_code_uc') \
                .map(lambda x: [x[i] for i in range(0,6)])
            if concat == '':
                concat = in_f
            else:
                temp = concat
                concat = sc.union([temp, in_f]).cache()


# join concatenated files with stores data over 'store_code_uc'
l = stores \
    .map(lambda x: (x[0], [x[i] for i in range(len(x)) if i != 0]))

r = concat \
    .map(lambda x: (x[0], [x[i] for i in range(len(x)) if i != 0]))

# resulting RDD will have the following cols:
# ['store_code_uc', 'upc', 'week_end', 'units', 'prmult', 'retailer_code', 'store_zip3']
result = r.join(l) \
    .map(lambda x: numpy.hstack((x[0], numpy.hstack(x[1]).tolist())).tolist()) \
    .map(lambda x: ','.join(str(d) for d in x)) \
    .saveAsTextFile(sys.argv[3])

# TODO: (ben) determine number of parties that we'd like to split the data across,
# and either append the split to this script or write it into a new one



