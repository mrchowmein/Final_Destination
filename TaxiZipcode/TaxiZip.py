
import csv
import geocoder
import time

borderRangeList = [0]

with open('loctax.csv') as csvfile:
    readCSV = csv.reader(csvfile, delimiter=',')
    next(readCSV)

    #row id correspond to the row id in taxizonepolycoords
    currentRowID = 1
    start = 0
    lastValue = 0
    for row in readCSV:

        if int(row[1]) > currentRowID:
            print(row[1], currentRowID)
            borderRangeList.append(start)
            start = int(row[3])
            currentRowID = int(row[1])

        lastValueStart = row[3]
        lastValueEnd = 0
        j = 3
        while j < len(row):

            if row[j].isnumeric():
                lastValueEnd = int(row[j])
            else:
                break
            j+=1



    borderRangeList.append(lastValueStart)
    borderRangeList.append(lastValueEnd)

print(borderRangeList)
print(len(borderRangeList))

avgCordsDict = {}

with open('taxizonepolycoords.csv') as csvfile:
    readCSV = csv.reader(csvfile, delimiter=',')
    next(readCSV)

    #row id correspond to the row id in taxizonepolycoords
    lat = 0
    long = 0
    start = 0
    end = 0
    zipsProcessed = 0
    i = 1
    while i < len(borderRangeList)-1:
        start = int(borderRangeList[i])
        end = int(borderRangeList[i+1])

        count = 0
        avgCordsList = []
        for row in readCSV:
            if int(row[0])>= start and int(row[0]) < end:
                lat += float(row[1])
                long += float(row[2])
                count +=1
            else:
                lat /= count

                long /=count

                g = geocoder.arcgis([lat, long], method='reverse')
                postal = g.postal

                avgCordsList.append(postal)
                avgCordsList.append(lat)
                avgCordsList.append(long)
                avgCordsList.append(count)

                avgCordsDict[i] = avgCordsList
                zipsProcessed+=1
                print("zips processed: ",zipsProcessed)
                time.sleep(.01)

                count = 0
                lat = 0
                long = 0
                break
        i +=1


    print(avgCordsDict)


with open('taxiZoneZips.csv', mode='w') as csv_file:
    fieldnames = ['zoneId', 'zipcode', 'lat', 'long']
    writer = csv.DictWriter(csv_file, fieldnames=fieldnames)

    writer.writeheader()

    for key in avgCordsDict:
        writer.writerow({'zoneId': key, 'zipcode': avgCordsDict[key][0], 'lat': avgCordsDict[key][1], 'long': avgCordsDict[key][2] })


