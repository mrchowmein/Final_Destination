import requests
import time
import geocoder
import csv

nystationJson = requests.get('https://gbfs.citibikenyc.com/gbfs/es/station_information.json').json()


stationDict = {}

for each in nystationJson['data']['stations']:
    #print(each['name'])
    id = int(each['station_id'])
    g = geocoder.arcgis([each['lat'], each['lon']], method='reverse')
    postal = g.postal
    print(each['name'], each['lat'], each['lon'], postal)
    stationDict[id] = [each['name'], each['lat'], each['lon'], postal]
    time.sleep(.01)

import csv

with open('stationZip.csv', mode='w') as csv_file:
    fieldnames = ['stationId', 'zipcode', 'lat', 'long']
    writer = csv.DictWriter(csv_file, fieldnames=fieldnames)

    writer.writeheader()

    for key in stationDict:
        writer.writerow({'stationId': key, 'zipcode': stationDict[key][3], 'lat': stationDict[key][1], 'long': stationDict[key][2] })
