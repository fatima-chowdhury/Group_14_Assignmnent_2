from csv import reader
from pyspark import SparkContext
from datetime import datetime

sc = SparkContext(appName="hw2")
sc.setLogLevel("ERROR")

# read input data from HDFS to create an RDD
data = sc.textFile("hdfs://group14-1:54310/hw1-input/") 

# split each line correctly as CSV
splitdata = data.mapPartitions(lambda x: reader(x))

# drop header
header = splitdata.first()
rows = splitdata.filter(lambda x: x != header)

# keep rows where crime type (OFNS_DESC) is not blank
# OFNS_DESC is index 7 in the given dataset
rows = rows.filter(lambda x: len(x) > 7 and x[7].strip() != "")

# --- filter month = July using RPT_DT at index 1 --

def is_july(dt):
    """
    True if RPT_DT parses and month == 7, else False.
    Supports formats like YYYY-MM-DD or MM/DD/YYYY.
    Non-parsable → False.
    """
    s = dt.strip()
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y"):
        try:
            return datetime.strptime(s, fmt).month == 7
        except:
            pass
    return False

july_rows = rows.filter(lambda x: len(x) > 1 and is_july(x[1]))

# --- map to (crime,1) and count --
pairs = july_rows.map(lambda x: (x[7].strip(), 1))
counts = pairs.reduceByKey(lambda a,b: a+b)

# --- sort descending by count --
sorted_counts = counts.sortBy(lambda kv: (-kv[1], kv[0]))

# --- take top 3 
top3 = sorted_counts.take(3)

print("Top 3 crimes in July:")
for rank,(crime,c) in enumerate(top3,1):
    print(f"{rank}. {crime} — {c}")

sc.stop()
