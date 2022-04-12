from pyspark import SparkContext

# Part 1

sc = SparkContext(appName="ass2")
temp_file = sc.textFile("BDA/input/temperature-readings.csv")
lines = temp_file.map(lambda line: line.split(";"))

# Extract year and month, and temperature
yearmonth_temp=lines.map(lambda x: (x[1][0:7], float(x[3])))

# Only include samples from 1950 to 2014, that are 10 degrees or above
yearmonth_temp=yearmonth_temp.filter(lambda x: int(x[0][0:4]) >= 1950 and int(x[0][0:4]) <=2014 and x[1] >= 10)

# Count by associating each year and month-sample with 1 and adding all up that belong to same key (year and month)
yearmonth_count=yearmonth_temp.map(lambda x: (x[0], 1))
yearmonth_count=yearmonth_count.reduceByKey(lambda a, b: a+b)

# Extract year, month and count
year_month_count=yearmonth_count.map(lambda x: (x[0][0:4], x[0][5:7], x[1]))

year_month_count.saveAsTextFile("BDA/output/count1")

# Part 2

# Extract station, year and month, and temperature
yearmonth_temp_distinct = lines.map(lambda x: (x[0], x[1][0:7], float(x[3])))

# Filter 
yearmonth_temp_distinct = yearmonth_temp_distinct.filter(lambda x: int(x[1][0:4]) >= 1950 and int(x[1][0:4]) <=2014 and x[2] >= 10)

# Set station and month as key, and 1 as value
yearmonth_count_distinct = yearmonth_temp_distinct.map(lambda x: ((x[0], x[1]), 1))

# Remove duplicate readings for same station in same month
yearmonth_count_distinct = yearmonth_count_distinct.reduceByKey(lambda a, b: a)

# Extract month as key
yearmonth_count_distinct = yearmonth_count_distinct.map(lambda x: (x[0][1], x[1]))

# Count for year and month
yearmonth_count_distinct= yearmonth_count_distinct.reduceByKey(lambda a, b: a+b)

# Extract year, month and count
year_month_count_distinct=yearmonth_count_distinct.map(lambda x: (x[0][0:4], x[0][5:7], x[1]))

year_month_count_distinct.saveAsTextFile("BDA/output/count2")

