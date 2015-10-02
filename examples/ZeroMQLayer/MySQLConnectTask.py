#!/usr/bin/python
import MySQLdb
import time
import json

'''
conf file
'''
metrics = ["Danville.Temperature", "Milton.Dissolved Oxygen"]
table = "sample_data"
start = 1373860800
end = 1405396800

jsonForm = [{"metrics":metrics},{"start":start},{"end":end},{"dbType":"MySQL"},{"extra":{"table":table}}]


# Keyi Database
# metrics = ["CR800_1.WaterLevel", "CR800_2.BattV"]
# start = 1405051200
# end = 1405531332
# dbType = "KeyiDB"
# {"extra": queries}


def getData(jsonForm):

    metrics = jsonForm[0]["metrics"]
    start = jsonForm[1]["start"]
    end = jsonForm[2]["end"]
    dbType = jsonForm[3]["dbType"]
#'''
#    if dbType == "KeyiDB":
#        aggregator = jsonForm[4]["extra"]["aggregator"]
#        downsample = jsonForm[4]["extra"]["downsample"]
#        rate = jsonForm[4]["extra"]["rate"]
#        tags = jsonForm[4]["extra"]["tags"]
#'''
    if dbType == "MySQL":
        table = jsonForm[4]["extra"]["table"]

        # Open database connection
        db = MySQLdb.connect("db.eg.bucknell.edu","sri","Hee1quai")

        # prepare a cursor object using cursor() method
        cursor = db.cursor()

        # use metrics to get site and parameter info
        siteIDs = []
        paramIDs = []
        for metric in metrics:
            siteID_query = "SELECT id FROM sri.sample_site WHERE name='"+metric.split(".")[0]+"'"
            cursor.execute(siteID_query)
            siteIDs += [cursor.fetchone()[0]]

            paramID_query = "SELECT id FROM sri.sample_parameter WHERE name='"+metric.split(".")[1]+"'"
            cursor.execute(paramID_query)
            paramIDs += [cursor.fetchone()[0]]


        # Command to query data
        temp = "s"+str(siteIDs[0]) + "p"+str(paramIDs[0])
        # use for every sample value
##        sql = "SELECT "+temp+".datetime as date, "
        # use for daily average value
        sql = "SELECT DATE(FROM_UNIXTIME("+temp+".datetime)) as date, "

        for i in range(len(metrics)):
            temp = "s"+str(siteIDs[i]) + "p"+str(paramIDs[i])
            sql += "AVG(" + temp + ".sample) AS sample" + temp + ", "

        sql = sql[0:-2]
        sql += " FROM "
        for i in range(len(metrics)):
            temp = "s"+str(siteIDs[i]) + "p"+str(paramIDs[i])
            sql += "sri."+table+" AS " + temp + ", "

        sql = sql[0:-2]  
        sql += " WHERE "
        count = 0
        for i in range(len(metrics)):
            temp = "s"+str(siteIDs[i]) + "p"+str(paramIDs[i])
            if (count > 0):
                sql += "AND "
            sql += temp + ".site_id="+str(siteIDs[i])+" "
            count += 1
            sql += "AND " + temp + ".parameter_id="+str(paramIDs[i])+" "

        for i in range(len(metrics)):
            temp = "s"+str(siteIDs[i]) + "p"+str(paramIDs[i])
            if i == 0:
                sql += "AND " + temp + ".datetime <= " + str(end) + " "
                sql += "AND " + temp + ".datetime >= " + str(start) + " "
            else:
                sql += "AND " + temp + ".datetime = " + "s"+str(siteIDs[i-1])+"p"+str(paramIDs[i-1]) + ".datetime "
        sql += "GROUP BY date"

##        print sql

        cursor.execute(sql)

        num_fields = len(cursor.description)
        ##fieldnames = [i[0] for i in cursor.description]

        results = cursor.fetchall()
        all_data = []

        for i in range(num_fields-1):
            all_data += [{}]
            all_data[i][metrics[i]] = []
            
        for j in range(len(results)):
            row = results[j]
            # ignore index 0, which is the date
            for i in range(num_fields-1):
                all_data[i][metrics[i]] += [{}]
                # use for sample
##                all_data[i][metrics[i]][j]["x"] = row[0]
                # use for daily average
                all_data[i][metrics[i]][j]["x"] = time.mktime(row[0].timetuple())
                all_data[i][metrics[i]][j]["y"] = row[i+1]

        # disconnect from server
        db.close()

        #print all_data
        return all_data


if __name__ == "__main__":
    print getData(jsonForm)
