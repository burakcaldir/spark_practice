import pyspark
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql import SparkSession
import feather

# Set up environment and start spark session
conf = pyspark.SparkConf().set('spark.driver.host','127.0.0.1')
sc = pyspark.SparkContext(master='local', appName='myAppName',conf=conf)
sqlContext = SQLContext(sc)

#######################
sparksesh = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

######################
# Call data from csv quick and dirty

def make_spark_data(selection, filepath_csv):
    if(selection == "pyspark"):
        # This makes a spark dataframe
        exp = sparksesh.read.option("header", "true").csv(filepath_csv) 
    elif(selection == "RDD"): 
        # This makes an RDD
        exp = sc.textFile(filepath_csv)
    return(exp)

#sp_data = make_spark_data(selection = "pyspark", filepath_csv = "")

######################
# Load data from a feather file (just spark df)

sampledata = feather.read_dataframe("ut_grad_earn.feather")
sp_data = sqlContext.createDataFrame(sampledata)

######################
# Look at the data

print(type(sp_data))
print(sp_data.schema.names)

###################
# These tasks work on RDD and on spark DF

print(sp_data.count()) #number of rows
print(sp_data.first()) # first row

# Other attributes: distinct, filter, foreach, groupBy, histogram, join, max, 
# mean, min, name, randomSplit, reduce, sample, saveAsPickleFile, saveAsTextFile,
# sortBy, stats, stdev, subtract (removing records), sum, take (like head), top

####################
# Cut a list of rows and screw around with that
three_rows = sp_data.take(25)

# Data is returned in list
print(type(three_rows))

##################
# Spark SQL tests #

# Filter to remove empty records
no_na_sp = sp_data.filter(sp_data.cellcount > 0)

# Select and show only certain columns
print(no_na_sp.select(["p25_earnings", "p50_earnings"]).show())