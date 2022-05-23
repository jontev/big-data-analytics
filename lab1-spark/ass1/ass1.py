from pyspark import SparkContext

def max_temp(a, b):
    if a >= b:
        return(a)
    else:
        return(b)

sc = SparkContext(appName="ass1")
temp_file = sc.textFile("BDA/input/temperature-readings.csv")
lines = temp_file.map(lambda line: line.split(";"))

# Extract year and temperature
year_temp=lines.map(lambda x: (x[1][0:4], float(x[3])))

# Only include samples from 1950 - 2014
year_temp=year_temp.filter(lambda x: int(x[0]) >= 1950 and int(x[0]) <=2014)

# Find max for each year
max_temp=year_temp.reduceByKey(max_temp)

# Sort descending by temperature
max_temp_sorted=max_temp.sortBy(ascending=False,keyfunc=lambda k: k[1])

max_temp_sorted.saveAsTextFile("BDA/output/max_temp")

