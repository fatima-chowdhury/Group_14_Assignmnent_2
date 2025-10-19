#!/usr/bin/env python2.7
from pyspark import SparkContext
from csv import reader

# Initialize SparkContext
sc = SparkContext(appName="hw2")
sc.setLogLevel("ERROR")

# Read data from HDFS or local path
# Replace path as needed
data = sc.textFile("hdfs://group14-1:54310/hw1-input/")

# Parse CSV safely
splitdata = data.mapPartitions(lambda x: reader(x))

# Filter out header row
# The header usually contains "CMPLNT_NUM" or "BORO_NM"
splitdata = splitdata.filter(lambda x: len(x) > 13 and x[13] != "BORO_NM")

# Columns of interest:
# boro_nm (Borough Name) -> index 13
BOROS = ("BRONX", "BROOKLYN", "MANHATTAN", "QUEENS", "STATEN ISLAND")

# Extract borough and count
def get_boro(row):
    try:
        boro = row[13].strip().upper()
        if boro in BOROS:
            return (boro, 1)
    except:
        pass
    return None

boro_counts = (
    splitdata.map(get_boro)
             .filter(lambda x: x is not None)
             .reduceByKey(lambda a, b: a + b)
)

# Find borough with most crimes
most_crime = boro_counts.takeOrdered(1, key=lambda x: -x[1])[0]

print("RESULT\t{}\t{}".format(most_crime[0], most_crime[1]))

sc.stop()
