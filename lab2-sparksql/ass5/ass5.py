from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql import functions as F

def max_temp(a, b):
    if a >= b:
        return(a)
    else:
        return(b)

sc = SparkContext(appName="ass5")
temp_file = sc.textFile("BDA/input/precipitation-readings.csv")
station_file = sc.textFile("BDA/input/stations-Ostergotland.csv")

lines = temp_file.map(lambda line: line.split(";"))
station_lines = station_file.map(lambda line: line.split(";"))
precReadings=lines.map(lambda p : (p[0], p[1].split("-")[0], p[1].split("-")[1], float(p[3])))
stationReadings = station_lines.map(lambda p : (p[0], p[1]))
precReadingsString = ["station", "year", "month", "value"]
stationReadingsString = ["station", "name"]

sqlContext = SQLContext(sc)
schemaPrecReadings=sqlContext.createDataFrame(precReadings, precReadingsString)
schemaPrecReadings.registerTempTable("precReadings")
schemaStationReadings=sqlContext.createDataFrame(stationReadings, stationReadingsString)
schemaStationReadings.registerTempTable("stationReadings")

schemaPrecReadingsOst = schemaPrecReadings.join(schemaStationReadings, on=["station"], how="inner")

schemaPrecReadingsAvg=schemaPrecReadingsOst.filter(schemaPrecReadingsOst.year >= 1993).filter(schemaPrecReadingsOst.year <= 2016).groupBy(["station", "year", "month"]).sum().groupBy(["year", "month"]).avg().orderBy(['year', 'month'], ascending=[0, 0])
output = schemaPrecReadingsAvg.rdd
output.saveAsTextFile("BDA/output/avg")


