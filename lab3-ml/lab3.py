from __future__ import division
from math import radians, cos, sin, asin, sqrt, exp
from datetime import datetime
from pyspark import SparkContext

sc = SparkContext(appName="lab_kernel")

def haversine(lon1, lat1, lon2, lat2):
    """
    Calculate the great circle distance between two points
    on the earth (specified in decimal degrees)
    """

    # convert decimal degrees to radians

    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

    # haversine formula

    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1)*cos(lat2)*sin(dlon/2)**2
    c = 2*asin(sqrt(a))
    km = 6367*c
    return km

def date_diff(date1, date2):
    date1 = datetime.strptime(date1, '%Y-%m-%d')
    date2 = datetime.strptime(date2, '%Y-%m-%d')

    diff = (date1 - date2).days % 365

    if diff > 182:
        diff = (date2 - date1).days % 365

    return diff

def time_diff(time1, time2):
    time1 = datetime.strptime(time1, '%H:%M:%S')
    time2 = datetime.strptime(time2, '%H:%M:%S')
    
    diff_day = (time1-time2).days

    mod_number = 60*60*24/2
    
    if diff_day < 0:
        diff=(time2 - time1).seconds % mod_number
    else:
        diff=(time1 - time2).seconds % mod_number

    return diff

def kernel(pred, sample):
    
    kernel_dist = exp(-(haversine(pred[0][0], pred[0][1], sample[0][0], sample[0][1])**2)/h_distance)
    kernel_date = exp(-(date_diff(pred[1], sample[1])**2)/h_date)
    kernel_time = exp(-(time_diff(pred[2], sample[2])**2)/h_time)
   
    #kernel_sum = kernel_dist + kernel_date + kernel_time
    kernel_sum = kernel_dist * kernel_date * kernel_time

    return kernel_sum

h_distance = 50# Up to you
h_date = 100# Up to you
h_time = 700000# Up to you
a = 58.4274 # Up to you
b = 14.826 # Up to you
date = "1998-09-21" # Up to you

stations = sc.textFile("BDA/input/stations.csv")
temps = sc.textFile("BDA/input/temperature-readings.csv")

station_lines = stations.map(lambda line: line.split(";")).cache()
temp_lines = temps.map(lambda line: line.split(";")).cache()

station_lat_long = station_lines.map(lambda p: (p[0], (float(p[3]), float(p[4]))))
station_lat_long.cache()
station_date_time_temp = temp_lines.map(lambda p: (p[0], (p[1], p[2], float(p[3]))))
station_date_time_temp.cache()

station_date_time_temp = station_date_time_temp.filter(lambda x: datetime.strptime(x[1][0], '%Y-%m-%d') < datetime.strptime(date, '%Y-%m-%d'))
station_date_time_temp.cache()

station_lat_long = station_lat_long.collectAsMap()
bc = sc.broadcast(station_lat_long)

station_lat_long_date_time_temp = station_date_time_temp.map(lambda x: (x[0], (bc.value[x[0]], x[1][0], x[1][1], x[1][2])))
station_lat_long_date_time_temp.cache()

for i, time in enumerate(["00:00:00", "22:00:00", "20:00:00", "18:00:00", "16:00:00", "14:00:00","12:00:00", "10:00:00", "08:00:00", "06:00:00", "04:00:00"]):

    kernels = station_lat_long_date_time_temp.map(lambda x: (kernel(((a, b), date, time), (x[1][0], x[1][1], x[1][2])), x[1][3]))
    kernels.cache()
    kernels = kernels.map(lambda x: (x[0]*x[1], x[0]))
    kernels.cache()
    kernels = kernels.reduce(lambda a, b: (a[0] + b[0], a[1] + b[1]))
    
    temp = kernels[0]/kernels[1]
    print(time, temp)
    
