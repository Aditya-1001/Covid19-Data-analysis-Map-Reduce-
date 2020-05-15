# Covid19-Data-analysis-Map-Reduce-
Covid19 Data analysis with Hadoop map-reduce and spark 

The program is to do the analysis of covid data using map reduce in hadoop and spark.

Java is used for hadoop and python for spark.

The task were executed using hadoop and spark with docker.

To get the data execute the following command : wget http://bmidb.cs.stonybrook.edu/publicdata/cse532-s20/covid19_full_data.csv

Import the data on hadoop cloud using : hdfs dfs -put covid19_full_data.csv /InputData/

To execute the Covid19_1.java in hadoop use the following command : hadoop Covid19.jar Covid19_1 /cse532/input/covid19_full_data.csv [true/false] /cse532/output/

True/False is used to include/exclude the cumulative covid cases of world.

Covid19_3.java is using distributed cache in hadoop. To run the program command is : hadoop jar Covid19_3.jar /covid19_full_data.csv /

Spark Program Run command : spark-submit /Covid19_3.py /covid19_full_data.csv
