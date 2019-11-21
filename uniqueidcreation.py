import numpy as np
import pandas as pd
import time as time
from math import radians, sqrt, sin, cos, atan2

directory = 'C:/Users/varsha/repos/arcgis2/MaintData/11-20-19'

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


read_cluster = pd.read_csv(directory + '/1min_clusters_id.csv')

sort_cluster = read_cluster.sort_values(by=['TIMESTART','FROM_XPOS'])
sort_cluster.reset_index(inplace=True)
#print(sort_cluster)
resall_data = []
M_data = []
value = 1
toggle = 0
count = 1
for i in range(len(sort_cluster)):
    if toggle == 0:
        key = "UID_" + str(value)
        M_data.append(key)
        M_data.append(sort_cluster['TIMESTART'][i])
        M_data.append(sort_cluster['FROM_XPOS'][i])
        M_data.append(sort_cluster['FROM_YPOS'][i])
        M_data.append(sort_cluster['FROM_MEASURE'][i])
        toggle = 1
    try:
        if(sort_cluster['CLUSTER_ID'][i] == sort_cluster['CLUSTER_ID'][i+1]): # and ((sort_cluster['ROUTE_ID'][i] == sort_cluster['ROUTE_ID'][i+1]) or (sort_cluster['CLUSTER_ID'][i] == sort_cluster['CLUSTER_ID'][i+1]))):
            if int(haversine(sort_cluster['FROM_YPOS'][i],sort_cluster['FROM_XPOS'][i],sort_cluster['FROM_YPOS'][i+1],sort_cluster['FROM_XPOS'][i+1])) < 0.7 :
                count = count+1
                continue
            else:
                value=value+1
        else:
            value = value +1
    except KeyError:
        break

    M_data.append(sort_cluster['TO_XPOS'][i])
    M_data.append(sort_cluster['TO_YPOS'][i])
    M_data.append(sort_cluster['TO_MEASURE'][i])
    M_data.append(sort_cluster['TIMEEND'][i])
    M_data.append(sort_cluster['ROUTE_ID'][i])
    M_data.append(count)
    resall_data.append(M_data)
    M_data = []
    toggle = 0
    count = 1


join = pd.DataFrame(np.array(resall_data),
                    columns=['Unique_ID','STARTTIME', 'FROM_XPOS','FROM_YPOS','FROM_MEASURE','TO_XPOS','TO_YPOS','TO_MEASURE', 'ENDTIME','ROUTE_ID','TRUCK_COUNT'])

#join_sort = join.sort_values(by=['ID','STARTTIME'])
join.to_csv(directory + '/uniqueid_operations.csv', index=False, header=True)
