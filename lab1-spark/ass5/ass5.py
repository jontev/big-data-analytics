from pyspark import SparkContext

sc = SparkContext(appName="ass5")
prec_file = sc.textFile("BDA/input/precipitation-readings.csv")
station_file = sc.textFile("BDA/input/stations-Ostergotland.csv")

station_lines = station_file.map(lambda line: line.split(";"))

# Create list of stations in Ostergotland
stations = station_lines.map(lambda x: x[0])
stations = stations.collect()

prec_lines = prec_file.map(lambda line: line.split(";"))

# Extract station, date and precipiation
station_date_prec = prec_lines.map(lambda x: (x[0], x[1], float(x[3])))

# Include only samples from 1993 to 2016
station_date_prec = station_date_prec.filter(lambda x: int(x[1][0:4]) >= 1993 and int(x[1][0:4]) <= 2016)

# Include only samples from Ostergotland
station_date_prec = station_date_prec.filter(lambda x: x[0] in stations)

# Set station and month as key and precipitation as value
stationmonth_prec = station_date_prec.map(lambda x: ((x[0], x[1][0:7]), x[2]))

# Sum precipitation for each station and month
station_month_sum = stationmonth_prec.reduceByKey(lambda a, b: a+b)

# Month as key, sum for different stations and 1 as value
month_stationsum_one = station_month_sum.map(lambda x: (x[0][1], (x[1], 1)))

# Sum sums and count stations for each month
month_stationsum_countofstations = month_stationsum_one.reduceByKey(lambda a, b: (a[0]+b[0], a[1]+b[1]))

# Calculate monthly average
month_avg = month_stationsum_countofstations.map(lambda x: (x[0], x[1][0]/x[1][1]))

month_avg.saveAsTextFile("BDA/output/avg")

