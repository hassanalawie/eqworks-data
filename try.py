# Importing For map
import folium
# Importing for data analysis/organization
import pandas
# Importing for distance calculations
import geopy
import math
# Pyspark libraries
from pyspark.sql import functions
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql import Row
# For Standard Deviation and Mean
from statistics import mean
from statistics import stdev 
# For legend on map
import branca.colormap as cm

# Creating empty arrays
distance1 = []
distance2 = []
distance3 = []
distance4 = []
closest = []
POI = []
POI1= []
POI2= []
POI3= []
POI4= []
c=0



def distance(lat1,long1, lat2, long2):
    import geopy.distance

    c1 = (lat1,long1)
    c2 = (lat2, long2)

    return geopy.distance.geodesic(c1, c2).km
def radfinder(x):
    if x == 0:
        rad = rad1
    elif x == 2:
        rad = rad3
    elif x==3:
        rad = rad4
    else:
        rad = 0
    return float(rad)
def colorgen(r):
    x = (r+10)*10
    return f'rgb(255,{x},{x})'
def ratioget(c):
    if c == 1:
        return r1
    elif c == 2:
        return r2
    else:
        return r3

spark = SparkSession.builder.getOrCreate()
data_path = "/Users/hassanalawie/desktop"
file_path= data_path + "/Datasample.csv"
file_path2= data_path + "/POIList.csv"

df1 = spark.read.format("csv").option("header","true").load(file_path)
df1.dropDuplicates([' TimeSt', 'Latitude', 'Longitude'])

data = pandas.read_csv(file_path)
data2 = pandas.read_csv(file_path2)
 

lat = list(data['Latitude'])
lon = list(data['Longitude'])
lat2 = list(data2[' Latitude'])
lon2 = list(data2['Longitude'])



for a,b in zip(lat, lon):
    w = distance(a,b, lat2[0], lon2[0])
    x = distance(a,b, lat2[1], lon2[1])
    y = distance(a,b, lat2[2], lon2[2])
    z = distance(a,b, lat2[3], lon2[3])
    distance1.append(w)
    distance2.append(x)
    distance3.append(y)
    distance4.append(z)
    closest.append(min(w,x,y,z))
    if min(w,x,y,z)==w:
        POI.append("POI1")
        POI1.append(w)
    elif min(w,x,y,z)==x:
        POI.append("POI2")
        POI2.append(x)
    elif min(w,x,y,z)==y:
        POI.append("POI3")
        POI3.append(y)
    else:
        POI.append("POI4")
        POI4.append(z)

df3 = spark.createDataFrame([(l,) for l in closest], ['Closest'])
df2 = spark.createDataFrame([(l,) for l in POI], ['POI'])
df1.show()
df2.show()
df3.show()


df1 = df1.withColumn("row_idx", monotonically_increasing_id())
df2 = df2.withColumn("row_idx", monotonically_increasing_id())

print (mean(POI1))
print (stdev(POI1))
rad1= max(POI1)
density1 = (len(POI1))/((math.pi)*(rad1**2))
print (density1)
print (mean(POI3))
print (stdev(POI3))
rad3= max(POI3)
density3 = (len(POI3))/((math.pi)*(rad3**2))
print (density3)
print (mean(POI4))
print (stdev(POI4))
rad4= max(POI4)
density4 = (len(POI4))/((math.pi)*(rad4**2))
print (density4)
print(rad1,rad3,rad4)
Pt = len(POI1)+len(POI2)+len(POI3)

print(len(POI1))
print(len(POI1))
print(len(POI1))


r1 = (20*len(POI1))/(Pt)
r2 = (20*len(POI3))/(Pt)
r3 = (20*len(POI4))/(Pt)

map = folium.Map([43,-80], zoom_start = 6, tiles = "OpenStreetMap")

colormap = cm.LinearColormap(colors=['red','pink'])
colormap = colormap.to_step(index=[10, 5, 0, -5, -10])
colormap.caption = 'Population'
colormap.add_to(map)

fg = folium.FeatureGroup(name="My Map")
fgp = folium.FeatureGroup(name="POI")


for lt, ln in zip(lat,lon):
    fg.add_child(folium.CircleMarker(location = [lt,ln], radius = 2,
    color = 'black', fill = True, fill_opacity=0.7))
for lt, ln in zip(lat2,lon2):
    fgp.add_child(folium.Circle(location = [lt,ln], radius = radfinder(c)*1000,
    color = colorgen(ratioget(c+1)), fill = True, fill_opacity=0.4))
    c+=1



map.add_child(fg)
map.add_child(fgp)

map.save("MapF.html")