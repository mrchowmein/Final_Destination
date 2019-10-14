import csv


stationDict = {}
sampleDict ={}

#read in station lookup table
with open('../CitiBikeZipcode/stationZip.csv') as csvfile:
    readCSV = csv.reader(csvfile, delimiter=',')
    next(readCSV)

    for row in readCSV:
        stationId = int(row[0])
        postal = row[1]
        lat = row[2]
        long = row[3]
        if postal != "":
            stationDict[stationId] = [lat, long, postal]

#readinsample
with open('./RefSource/sample2019060600.csv') as csvfile:
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
            if startZip != "00000" or endZip != "00000":
                sampleDict[key] = 1


#save refcheckfile
with open('./RefSource/2019060601RefCount.csv', mode='w') as csv_file:
    fieldnames = ['key', 'count']
    writer = csv.DictWriter(csv_file, fieldnames=fieldnames)

    writer.writeheader()

    for key in sampleDict:
        writer.writerow({'key': key, 'count': sampleDict[key]})
