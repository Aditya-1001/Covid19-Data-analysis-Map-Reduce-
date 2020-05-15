#Command : spark-submit /Covid19_3.py /covid19_full_data.csv hdfs://namenode:8020/populations.csv /
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

if __name__ == "__main__":

    startTime = time.time()

    if len(sys.argv) != 4:
        print("Usage: SparkCovid19_2.py <input file> <cache file> <output file>", file=sys.stderr)
        sys.exit(-1)
    
    PathOutput = sys.argv[3] + "/pyspark_task_2.txt"


    spark = SparkSession\
    .builder\
    .appName("SparkCovid19_2")\
    .getOrCreate()


    sc = spark.sparkContext

    e = []

    populations = spark.read.format("CSV").option("header","true").load(sys.argv[2])


    pop = populations.collect()    

    length = len(pop)
    updated = {}

    for i in range(0, length):
        updated[pop[i]['location'].replace("_"," ")] = pop[i]['population']
    

    broadcastData = sc.broadcast(updated).value


    covid = spark.read.format("CSV").option("header","true").load(sys.argv[1])

    y = covid.rdd.map(lambda r: r)

    counts = y \
    .map(lambda x: (x["location"], int(x["new_cases"]))) \
    .reduceByKey(add)

    output = counts.collect()
    output = sorted(output, key=lambda x:x[0])

    output_file = ''

    if os.path.exists(PathOutput):
        os.remove(PathOutput)
    
    output_file = open(PathOutput, "a")

    for (i, val) in output:
        if i in broadcastData and broadcastData[i] != None:
            PerMillion = float(float(val * 1000000) / float(broadcastData[i])) 
            print("%s %f" % (i, PerMillion))
            output_file.write("%s %f\n" % (i, PerMillion))


    endTime = time.time()

    millis = endTime - startTime

    print("Execution Time = %i milliseconds"%(millis))
    spark.stop()