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

# Filter for DANGEROUS WEAPONS crimes in July
def is_july_dangerous_weapons(row):
    try:
        ofns_desc = row[7].strip().upper()
        rpt_date = row[5].strip()
        
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
count = filtered.count()

print("Number of DANGEROUS WEAPONS crimes reported in July: {}".format(count))

# Stop SparkContext
sc.stop()
