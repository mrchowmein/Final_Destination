import os
import requests
import time
import geocoder
import csv


nystationJson = requests.get('https://gbfs.citibikenyc.com/gbfs/es/station_information.json').json()

stationDict = {}

if os.path.exists('stationZip.csv') == True:
    print("Will use existing stationZip.csv")
    with open('stationZip.csv') as csvfile:
        readCSV = csv.reader(csvfile, delimiter=',')
        next(readCSV)

        for row in readCSV:
            stationId = int(row[0])
            postal = row[1]
            lat = row[2]
            long = row[3]
            if postal != "":
                print("adding: ", stationId, " ", postal)
                stationDict[stationId] = [lat, long, postal]


for each in nystationJson['data']['stations']:
    #print(each['name'])
    id = int(each['station_id'])
    if stationDict.get(id) == None:
        g = geocoder.arcgis([each['lat'], each['lon']], method='reverse')
        postal = g.postal
        print(each['name'], each['lat'], each['lon'], postal)
        stationDict[id] = [each['lat'], each['lon'], postal]
        time.sleep(.01)
    else:
        print("Skipping: ", id)



with open('stationZip.csv', mode='w') as csv_file:
    fieldnames = ['stationId', 'zipcode', 'lat', 'long']
    writer = csv.DictWriter(csv_file, fieldnames=fieldnames)

    writer.writeheader()

    for key in stationDict:
        writer.writerow({'stationId': key, 'zipcode': stationDict[key][2], 'lat': stationDict[key][0], 'long': stationDict[key][1] })
