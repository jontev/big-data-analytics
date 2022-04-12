from pyspark import SparkContext

sc = SparkContext(appName="ass3")
temp_file = sc.textFile("BDA/input/temperature-readings.csv")
lines = temp_file.map(lambda line: line.split(";"))

# Extract station, year and month, and temperature
station_yearmonth_temp=lines.map(lambda x: (x[0], x[1][0:7], float(x[3])))

# Include only samples from 1960 to 2014
station_yearmonth_temp=station_yearmonth_temp.filter(lambda x: int(x[1][0:4]) >= 1960 and int(x[1][0:4]) <=2014)

# Station station and month as key and temperature and 1 (to count) as value
stationyearmonth_temp=station_yearmonth_temp.map(lambda x: ((x[0], x[1]), (x[2], 1)))

# Sum temperatures for station and month, and count
stationyearmonth_sumtemp=stationyearmonth_temp.reduceByKey(lambda a, b: (a[0]+b[0], a[1]+b[1]))

# Calculate average for station and month by using sum and count
stationyearmonth_avg=stationyearmonth_sumtemp.map(lambda x: (x[0], x[1][0]/x[1][1]))

# Extract year, month, station and average
year_month_station_avg=stationyearmonth_avg.map(lambda x: (x[0][1][0:4], x[0][1][5:7], x[0][0], x[1]))

year_month_station_avg.saveAsTextFile("BDA/output/avg")

