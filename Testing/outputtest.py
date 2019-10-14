import csv
import geocoder
import time

stationDict = {}
sampleDict ={}


with open('../CitiBikeZipcode/stationZip.csv') as csvfile:
    readCSV = csv.reader(csvfile, delimiter=',')
    next(readCSV)

    for row in readCSV:
        stationId = int(row[0])
        postal = row[1]
        lat = row[2]
        long = row[3]
        if postal != "":
            #print("adding: ", stationId, " ", postal)
            stationDict[stationId] = [lat, long, postal]

with open('./Ref/sample1.csv') as csvfile:
    readCSV = csv.reader(csvfile, delimiter=',')
    next(readCSV)

    for row in readCSV:
        startZip = "00000"
        endZip = "00000"
        if int(row[3]) in stationDict:
            startZip = str(stationDict.get(int(row[3]))[2])
            if startZip == "0":
                startZip= "00000"

        if int(row[7]) in stationDict:
            endZip = str(stationDict.get(int(row[7]))[2])
            if endZip == "0":
                endZip= "00000"
        date = row[1].split(" ")[0]
        hour = row[1].split(" ")[1].split(":")[0]

        key = startZip+endZip+date+hour
        if key in sampleDict:
            val = sampleDict.get(key)
            sampleDict[key] = val + 1
        else:
            sampleDict[key] = 1

        #print(startZip, endZip, date, hour)
    print(sampleDict)
