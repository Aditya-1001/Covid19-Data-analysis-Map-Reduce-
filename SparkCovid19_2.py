#Command : spark-submit /Covid19_2.py /covid19_full_data.csv 2019-12-31 2020-04-08 /
from __future__ import print_function
import datetime as dt
import time
import sys
import os
from operator import add
from pyspark.sql.dataframe import DataFrame as DataFrame
from pyspark.sql import SparkSession
from pyspark import SparkContext 
from pyspark.sql import functions as F
from pyspark import SparkContext, broadcast

if __name__== "__main__":

    startTime = time.time()
    if len(sys.argv) != 5 :
        print("Usage: Covid19_2.py <input file> <start date> <end date> <output file>", file=sys.stderr)
        sys.exit(-1)
    
    pathOutput = sys.argv[4] + "/pyspark_task_1.txt"

    spark = SparkSession\
    .builder\
    .appName("Covid19_2")\
    .getOrCreate()

    sc = spark.sparkContext

    errors = []

    
    dates = ("2019-12-31", "2020-04-08", sys.argv[2], sys.argv[3])

    date_from, date_to, start, end = [s for s in dates]

    if start > end or start > date_to or end < date_from:
        print("incorrect arguments")
        sys.exit(-1)


    covid = spark.read.format("CSV").option("header","true").load(sys.argv[1])
    lines = covid.rdd.map(lambda r: r)
    y = lines.filter(lambda x : x[0] >= start).filter(lambda x : x[0] <= end)\
    .map(lambda x: (x["location"], int(x["new_deaths"]))) \
    .reduceByKey(add).sortByKey()
    output = y.collect()

    print("output",output)
   
    output = sorted(output, key=lambda x:x[0])
    output_file = ''
    if os.path.exists(pathOutput):
        os.remove(pathOutput)
    
    output_file = open(pathOutput, "a")

    for (i, val) in output:
        print("%s %d" % (i, int(val)))
        output_file.write("%s %d\n" % (i, val))       

    endTime = time.time()
    
    millis = endTime - startTime

    print("Execution Time = %i milliseconds"%(millis))

    spark.stop()