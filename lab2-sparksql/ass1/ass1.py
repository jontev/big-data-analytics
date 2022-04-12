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
tempReadings=lines.map(lambda p : (p[0], p[1].split("-")[0], float(p[3])))
tempReadingsString = ["station", "year", "value"]

sqlContext = SQLContext(sc)
schemaTempReadings=sqlContext.createDataFrame(tempReadings, tempReadingsString)
schemaTempReadings.registerTempTable("tempReadings")

schemaTempReadingsMax=schemaTempReadings.filter(schemaTempReadings.year > 1949).filter(schemaTempReadings.year < 2015).groupBy("year").agg(F.max('value').alias('yearmax')).orderBy(['yearmax'], ascending=[0])
schemaTempReadingsMin=schemaTempReadings.filter(schemaTempReadings.year > 1949).filter(schemaTempReadings.year < 2015).groupBy("year").agg(F.min('value').alias('yearmin')).orderBy(['yearmin'], ascending=[0])

maxoutput = schemaTempReadingsMax.rdd
minoutput = schemaTempReadingsMin.rdd

maxoutput.saveAsTextFile("BDA/output/max_temp")
minoutput.saveAsTextFile("BDA/output/min_temp")

