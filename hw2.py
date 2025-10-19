#!/usr/bin/env python2.7
from pyspark import SparkContext
from csv import reader
from datetime import datetime

# Initialize SparkContext
sc = SparkContext(appName="hw2")
sc.setLogLevel("ERROR")

# Read data from HDFS
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
def get_boro(row1):
    try:
        boro = row1[13].strip().upper()
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





# drop header
header = splitdata.first()
rows = splitdata.filter(lambda x: x != header)

# keep rows where crime type (OFNS_DESC) is not blank
# OFNS_DESC is index 7 in the given dataset
rows2 = rows2.filter(lambda x: len(x) > 7 and x[7].strip() != "")

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

july_rows = rows2.filter(lambda x: len(x) > 1 and is_july(x[1]))

# --- map to (crime,1) and count --
pairs = july_rows.map(lambda x: (x[7].strip(), 1))
counts2 = pairs.reduceByKey(lambda a,b: a+b)

# --- sort descending by count --
sorted_counts = counts2.sortBy(lambda kv: (-kv[1], kv[0]))

# --- take top 3 
top3 = sorted_counts.take(3)

print("Top 3 crimes in July:")
for rank,(crime,c) in enumerate(top3,1):
    print(f"{rank}. {crime} — {c}")





# Filter out header row based on column label content
splitdata = splitdata.filter(lambda x: len(x) > 7 and x[5] != "RPT_DT" and x[7] != "OFNS_DESC")

# Columns of interest:
# rpt_dt (Report Date) -> index 5
# ofns_desc (Offense Description) -> index 7

# Filter for DANGEROUS WEAPONS crimes in July
def is_july_dangerous_weapons(row3):
    try:
        ofns_desc = row3[7].strip().upper()
        rpt_date = row3[5].strip()
        
        # Only consider records that exactly match "DANGEROUS WEAPONS"
        if ofns_desc != "DANGEROUS WEAPONS":
            return False
        
        # Parse date as MM/DD/YYYY (the NYPD format)
        date_obj = datetime.strptime(rpt_date, "%m/%d/%Y")
        
        # Keep only records from July
        return date_obj.month == 7
    except:
        # Skip any malformed rows
        return False

# Apply filter
filtered = splitdata.filter(is_july_dangerous_weapons)

# Count the number of records
count3 = filtered.count()

print("Number of DANGEROUS WEAPONS crimes reported in July: {}".format(count3))



# Stop SparkContext
sc.stop()
