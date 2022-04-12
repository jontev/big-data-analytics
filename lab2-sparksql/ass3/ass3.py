from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql import functions as F

def max_temp(a, b):
    if a >= b:
        return(a)
    else:
        return(b)

sc = SparkContext(appName="ass1")
temp_file = sc.textFile("BDA/input/temperature-readings.csv")
lines = temp_file.map(lambda line: line.split(";"))
tempReadings=lines.map(lambda p : (p[0], p[1].split("-")[0], p[1].split("-")[1], float(p[3])))
tempReadingsString = ["station", "year", "month", "value"]

sqlContext = SQLContext(sc)
schemaTempReadings=sqlContext.createDataFrame(tempReadings, tempReadingsString)
schemaTempReadings.registerTempTable("tempReadings")

schemaTempReadingsMax=schemaTempReadings.filter(schemaTempReadings.year >= 1960).filter(schemaTempReadings.year <= 2014).groupBy(["year", "month", "station"]).avg().orderBy(['avg(value)'], ascending=[0])
output = schemaTempReadingsMax.rdd

output.saveAsTextFile("BDA/output/avg")


