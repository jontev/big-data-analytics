from pyspark import SparkContext

def max_temp(a, b):
    if a >= b:
        return a
    else:
        return b

sc = SparkContext(appName="ass4")
temp_file = sc.textFile("BDA/input/temperature-readings.csv")
prec_file = sc.textFile("BDA/input/precipitation-readings.csv")

temp_lines = temp_file.map(lambda line: line.split(";"))
prec_lines = prec_file.map(lambda line: line.split(";"))

# Extract station and temperature
station_temp = temp_lines.map(lambda x: (x[0], float(x[3])))

# Extract station and date as key and precipation as value
station_day_time_prec = prec_lines.map(lambda x:((x[0], x[1]), float(x[3])))

# Sum precipitation by day and station
station_day_time_prec = station_day_time_prec.reduceByKey(lambda a, b: a+b)

# Set station as key and precipiation as value
station_prec = station_day_time_prec.map(lambda x: (x[0][0], x[1]))

# Find max temperature and precipitation
station_maxtemp = station_temp.reduceByKey(max_temp)
station_maxprec = station_prec.reduceByKey(max_temp)

# Join by station
joined = station_maxtemp.join(station_maxprec)

# Extract station, temperature and precipitation
joined = joined.map(lambda x: (x[0], x[1][0], x[1][1]))

# Filter so only 25 - 30 and 100 - 200
output = joined.filter(lambda x: x[1] >= 25 and x[1] <= 30 and x[2] >= 100 and x[2] <= 200)

output.saveAsTextFile("BDA/output/maxtempandprec")

