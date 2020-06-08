import os.path
from collections import defaultdict
import datetime
import time

###Download data
timestr = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
todayDate = datetime.datetime.now().strftime("%m-%d-%y")

directory = 'C:/Users/varsha/repos/arcgis2/MaintData/' + todayDate
if not os.path.exists(directory):
   os.makedirs(directory)

###LRS Process
import pandas as pd
import numpy as np
import json
import requests
from math import radians, sqrt, sin, cos, atan2

def haversine(lat1,lon1,lat2,lon2):

    lat1 = radians(round(lat1,6))
    lon1 = radians(round(lon1,6))
    lat2 = radians(round(lat2,6))
    lon2 = radians(round(lon2,6))

    radius = 6373 # km

    dlat = lat1-lat2
    dlon = lon1-lon2

    a = sin(dlat/2) * sin(dlat/2) + cos(lat1) * cos(lat2) * sin(dlon/2) * sin(dlon/2)
    c = 2 * atan2(sqrt(a), sqrt(1-a))
    d = radius * c
    return d


def get_locations(x,y):
    d = []
    form = {'geometry':{'y':y, 'x':x}}
    d.append(form)

    locations = json.dumps(d, ensure_ascii=False)
    return d, locations

# Route Descriptor - High priority
route_desc_priority = {'S': 1, 'C': 2, 'M': 3, 'P': 4, 'I': 5}
# System Code - Medium Priority
route_code_priority = {'1': 1, '2': 2, '3': 3, '4': 4}
# Direction - Low Priority
route_dir_priority = {'N': 1, 'E': 1, 'W': 2, 'S': 2}


# M015546105E
def dominant_route_selector(route_id):
    # Exceptional case - Give these routes high priority.
    if route_id == 'S001910080W' or route_id == 'S001910080E':
        return 1
    # Route Criteria =  route_desc > route_code > route_number > route_dir > ramp
    key = int('{}{}{}{}'.format(route_desc_priority[route_id[0]], route_code_priority[route_id[5]], route_id[6:10],
                                route_dir_priority[route_id[10]]))
    if len(route_id) > 11:
        # If it is a RAMP, add extra weight
        key += 5
    #print(route_id, key)
    return key


def get_routeid(dat,x,y,rflag):

    dist1 = []
    dat = sorted(dat, key = lambda m : (dominant_route_selector(m[0])))
    if (len(dat) == 1):
        return [dat[0][0], dat[0][1]]

    loop_len = len(dat)

    for i in range(loop_len):
        a=[]
        a = haversine(y, x, dat[i][3], dat[i][2])
        dist1.append([a,i,dat[i][0]])

    x = sorted(dist1)

    if rflag == 1:
        val = x[0][1]
    else:
        if(x[0][2][:1] != 'S' and x[1][2][:1] != 'S'):
            val = x[2][1]
        elif(x[0][2][:1] != 'S'):
            val = x[1][1]
        else:
            val = x[0][1]

    return [dat[val][0], dat[val][1]]

def get_measure(location, tolerance, xpos,ypos,rflag):
    url = 'https://gis.iowadot.gov/rams/rest/services/lrs/MapServer/exts/LRSServer/networkLayers/0/geometryToMeasure?'
    data = {'f': 'json', 'locations': location, 'tolerance': str(tolerance), 'inSR': '4326'}
    #print(location)
    response = requests.post(url, data=data)
    r = response.json()
    #print(r)
    flag = 0
    # extract results
    resall = []
    for i in r['locations']:
        res = []
        if i['status'] == u'esriLocatingCannotFindLocation':
            routeid = '0'
            measure =0
            x = 0
            y=0
            res.append([routeid,measure,x,y])
            flag =1
        else:
            for j in i['results']:
                routeid = j['routeId'].encode('ascii', 'ignore')
                measure = j['measure']
                x = j['geometry']['x']
                y = j['geometry']['y']
                res.append([routeid, measure, x, y])
    resall.append(res)
    if flag == 1:
        routeid, mm = '0',0
    else:
        for i in range(len(resall)):
            routeid , mm = get_routeid(resall[i],xpos,ypos,rflag)


    return routeid, mm

start = time.time()
print("Downloading API data and running RouteID algo : ", start)
url = 'https://gis.iowadot.gov/agshost/rest/services/Winter_Operations/AVL_Plow_Pings_1_Hour/FeatureServer/0/query?where=1%3D1&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&distance=&units=esriSRUnit_Foot&relationParam=&outFields=*&returnGeometry=true&maxAllowableOffset=&geometryPrecision=&outSR=&having=&gdbVersion=&historicMoment=&returnDistinctValues=false&returnIdsOnly=false&returnCountOnly=false&returnExtentOnly=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&returnZ=false&returnM=false&multipatchOption=xyFootprint&resultOffset=&resultRecordCount=&returnTrueCurves=false&returnExceededLimitFeatures=false&quantizationParameters=&returnCentroid=false&sqlFormat=none&resultType=&featureEncoding=esriDefault&f=json'
#data = {'f': 'json'}
response = requests.get(url)
r = response.json()
# extract results
resall = []

for i in r['features']:
    res= []
    x = i['attributes']['Longitude']
    y = i['attributes']['Latitude']
    ping = i['attributes']['PingID']
    if ping is 0:
        continue
    logdt = i['attributes']['PingDateTime']
    logdt = logdt/1000
    ts = int(logdt)
    logdatetime = datetime.datetime.fromtimestamp(ts)
    label = i['attributes']['TruckID']
    velocity = i['attributes']['Velocity']
    givenRoute = i['attributes']['RouteFullName']
    segment = i['attributes']['SegmentID']
    res.append(x)
    res.append(y)
    res.append(ping)
    res.append(logdatetime.date())
    res.append(logdatetime.time())
    res.append(label)
    res.append(velocity)
    res.append(givenRoute)
    res.append(segment)
    resall.append(res)

joink = pd.DataFrame(np.array(resall), columns=['XPOSITION', 'YPOSITION','PINGID', 'DATE', 'TIME', 'LABEL', 'VELOCITY','GIVENROUTE','SEGMENT'])

joink.to_csv(directory + '/live_feed' + timestr + '.csv', mode='a', index=False, header=True)

df = pd.read_csv(directory + '/live_feed' + timestr + '.csv')

final = []
c = 0
for i in range(len(df)):
    rflag = 0
    print(c)
    c=c+1
    datall =[]
    xpos = df['XPOSITION'][i]
    ypos = df['YPOSITION'][i]
    if pd.isnull(df['GIVENROUTE'][i]):
        rflag = 1
    d, location = get_locations(round(xpos,6),round(ypos,6))
    routeid, measure = get_measure(location, 40, xpos,ypos,rflag)
    if(routeid == '0'):
        #print("Hi")
        continue
    datall.append(df['PINGID'][i])
    datall.append(df['LABEL'][i])
    datall.append(df['VELOCITY'][i])
    datall.append(df['XPOSITION'][i])
    datall.append(df['YPOSITION'][i])
    datall.append(df['SEGMENT'][i])
    datall.append(routeid)
    datall.append(df['GIVENROUTE'][i])
    datall.append(measure)
    datall.append(df['DATE'][i])
    datall.append(df['TIME'][i])
    final.append(datall)

joink = pd.DataFrame(np.array(final),
                    columns=['PINGID', 'LABEL', 'VELOCITY', 'XPOSITION', 'YPOSITION','SEGMENT','ROUTEID','GIVENROUTE', 'MEASURE', 'DATE', 'TIME'])

joink.to_csv(directory + '/live_feed_route_mm' + timestr + '.csv', mode='a', index=False, header=True)

end = time.time()

print("Time taken",end-start)

print("Overlay Route Event process: ")

# Import system modules
import arcpy

# Set workspace
arcpy.env.workspace = directory

#csv to table conversion
arcpy.TableToDBASE_conversion(['/live_feed_route_mm' + timestr + '.csv'], directory)

#csv to table conversion
arcpy.TableToDBASE_conversion(["SPEED_LIMIT.csv"], directory)


# Set local variables
in_tbl ='/live_feed_route_mm' + timestr + '.dbf'
in_props = "ROUTEID POINT MEASURE"  # reused as out event properties
ov_tbl = "SPEED_LIMIT.dbf"
ov_props = "ROUTE_ID LINE FROM_MEASU TO_MEASURE"
out_tbl = '/live_feed_route_mm_overlay+2.csv'

# Execute OverlayRouteEvents
arcpy.OverlayRouteEvents_lr (in_tbl, in_props, ov_tbl, ov_props, "INTERSECT", out_tbl, in_props)

end1 = time.time()

print("Time taken",end1-end)

print("Maintanence operation filtering.. ")

M_data = []
M_data = pd.read_csv(directory + '/live_feed_route_mm_overlay+2.csv',
                     usecols=['PINGID','LABEL', 'VELOCITY', 'XPOSITION', 'YPOSITION', 'ROUTEID', 'MEASURE',
                              'DATE', 'TIME', 'SPEED_LIMI'])
rams_AllData = []
toggle1 = 0
v_Id = 1

All_data = []
for data in range(len(M_data)):
    New_data = []
    speed = M_data['SPEED_LIMI'][data]
    if (M_data['VELOCITY'][data] < (0.35 * speed)):
        New_data.append(M_data['PINGID'][data])
        New_data.append(M_data['LABEL'][data])
        New_data.append(M_data['VELOCITY'][data])
        New_data.append(M_data['XPOSITION'][data])
        New_data.append(M_data['YPOSITION'][data])
        New_data.append(M_data['ROUTEID'][data])
        New_data.append(M_data['MEASURE'][data])
        New_data.append(M_data['DATE'][data])
        New_data.append(M_data['TIME'][data])
    else:
        continue
    All_data.append(New_data)

joink = pd.DataFrame(np.array(All_data),
                    columns=['PINGID','LABEL', 'VELOCITY', 'XPOSITION', 'YPOSITION', 'ROUTEID', 'MEASURE',
                              'DATE', 'TIME'])
join1 = joink.sort_values(by=['TIME'])
join1.to_csv(directory + '/live_feed_route_mm_updated+2.csv', mode='a', index=False, header=True)

end2 = time.time()

print("Time taken",end2-end1)

print("1min clustering ")

M_data = []
M_data = pd.read_csv(directory + "/live_feed_route_mm_updated+2.csv",
                     usecols=['PINGID','LABEL', 'VELOCITY', 'XPOSITION', 'YPOSITION', 'ROUTEID', 'MEASURE',
                              'DATE', 'TIME'])
flag = 0
count=0
i=0
mydict= defaultdict(dict)
percount=0
mydictsubs = defaultdict(list)
value = 0
while i < len(M_data):
    count = 1
    NewData = []
    if flag == 0:
        value = value + 1
        off_starttime = M_data['TIME'][i]
        starttime = str(off_starttime[:2]) + str(off_starttime[3:5]) + str(off_starttime[6:8])
        min = str(starttime)[2:4]
        umin = int(min) + 1
        if(int(str(umin)) == 60):
            hour = str(starttime)[:2]
            hour_u = int(hour)+1
            endtime = str(hour_u) + '00' + str(starttime)[4:6]
        elif (len(str(umin))==2):
            endtime = str(starttime)[:2] + str(umin) + str(starttime)[4:6]
        else:
            endtime = str(starttime)[:3] + str(umin) + str(starttime)[4:6]
        off_endtime = str(endtime[:2]) + ":"  +str(endtime[2:4]) + ":" + str(endtime[4:6])
    new_time = str(M_data['TIME'][i][:2]) + str(M_data['TIME'][i][3:5]) + str(M_data['TIME'][i][6:8])
    if int(new_time) <= int(endtime):
        flag = 1
        clusterid = "C_"+ str(value)
        label = M_data['LABEL'][i]
        key = label + "_" + str(umin)
        if key not in mydict.keys():
            routeid = str(M_data['ROUTEID'][i])
            NewData.append(clusterid)
            NewData.append(M_data['TIME'][i])
            NewData.append(M_data['TIME'][i])
            NewData.append(M_data['MEASURE'][i])
            NewData.append(M_data['MEASURE'][i])
            NewData.append(M_data['XPOSITION'][i])
            NewData.append(M_data['YPOSITION'][i])
            NewData.append(M_data['XPOSITION'][i])
            NewData.append(M_data['YPOSITION'][i])
            NewData.append(routeid)
            NewData.append(off_starttime)
            NewData.append(off_endtime)
            NewData.append(count)
            mydictsubs[routeid] = NewData
            mydict[key][routeid] = mydictsubs[routeid]
            #print(mydict)
            #print(mydict)
        else:
            routeid = str(M_data['ROUTEID'][i])
            if (mydict.get(key,{}).get(routeid) is None):
            #if routeid not in mydictsubs.keys():
                #print("route also new")
                NewData.append(clusterid)
                NewData.append(M_data['TIME'][i])
                NewData.append(M_data['TIME'][i])
                NewData.append(M_data['MEASURE'][i])
                NewData.append(M_data['MEASURE'][i])
                NewData.append(M_data['XPOSITION'][i])
                NewData.append(M_data['YPOSITION'][i])
                NewData.append(M_data['XPOSITION'][i])
                NewData.append(M_data['YPOSITION'][i])
                NewData.append(routeid)
                NewData.append(off_starttime)
                NewData.append(off_endtime)
                NewData.append(count)
                mydictsubs[routeid]= NewData
                mydict[key][routeid] = mydictsubs[routeid]
            else:
                #print("route also exists")
                #print(mydict.get(key))
                count = mydict.get(key, {}).get(routeid)[12]
                mydict.get(key, {}).get(routeid)[12] = count + 1
                mydict.get(key, {}).get(routeid)[2] = M_data['TIME'][i]
                mydict.get(key, {}).get(routeid)[4] = M_data['MEASURE'][i]
                mydict.get(key, {}).get(routeid)[7] = M_data['XPOSITION'][i]
                mydict.get(key, {}).get(routeid)[8] = M_data['YPOSITION'][i]
        i=i+1
    else:
        flag = 0



listf=[]
for key, values in mydict.items():
    for keys, value in values.items():
        list1 = []

        list1.append(value[0])
        list1.append(key.split("_")[0])
        list1.append(value[1])
        list1.append(value[2])
        list1.append(value[3])
        list1.append(value[4])
        list1.append(value[5])
        list1.append(value[6])
        list1.append(value[7])
        list1.append(value[8])
        list1.append(value[9])
        list1.append(value[10])
        list1.append(value[11])
        list1.append(value[12])
        listf.append(list1)

# print(listf)

joink = pd.DataFrame(np.array(listf),columns=['CLUSTER_ID','LABEL','ACT_TIME_S','ACT_TIME_E','FROM_MEASURE','TO_MEASURE','FROM_XPOS','FROM_YPOS','TO_XPOS','TO_YPOS','ROUTE_ID','TIMESTART','TIMEEND','COUNT'])

joink.to_csv(directory + '/1min_clusters_id_ST+2.csv', mode='a', index=False)

end3 = time.time()

print("process complete and file generated",end3-end2)

