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

# Filter out header row based on column label content
splitdata = splitdata.filter(lambda x: len(x) > 7 and x[5] != "RPT_DT" and x[7] != "OFNS_DESC")

# Columns of interest:
# rpt_dt (Report Date) -> index 5
# ofns_desc (Offense Description) -> index 7

# Filter for DANGEROUS WEAPONS and month == July
def is_july_dangerous_weapons(row):
    try:
        ofns_desc = row[7].strip().upper()
        rpt_date = row[5].strip()
        if ofns_desc == "DANGEROUS WEAPONS":
            # Some dates might be 'MM/DD/YYYY' or 'YYYY-MM-DD'
            try:
                date_obj = datetime.strptime(rpt_date, "%m/%d/%Y")
            except:
                try:
                    date_obj = datetime.strptime(rpt_date, "%Y-%m-%d")
                except:
                    return False
            return date_obj.month == 7
        return False
    except:
        return False

filtered = splitdata.filter(is_july_dangerous_weapons)

# Count the number of records
count = filtered.count()

print("Number of DANGEROUS WEAPONS crimes reported in July: {}".format(count))

sc.stop()
