from flask import Flask, render_template, request
from flask import jsonify
from pymongo import MongoClient
from bson.json_util import dumps

CrimeApp = Flask(__name__, template_folder='templates')


@CrimeApp.route('/', methods=['GET', 'POST'])
def home():
    if request.method == 'GET':
        return render_template("crimePortal.html")

    elif request.method == 'POST':
        client = MongoClient("mongodb://localhost:27017/")
        database = client['test']
        collection = database['Incidents']
        collection2 = database['Community_Station']
        collection3 = database['ReportedIncidents']

        # Case 1 - Find the distance between the top 5, highest crime communities and the nearest police stations
        if request.form['Submit'] == "FindByComID":
            query1 = request.form
            community_id = query1['CommunityID']
            community_id = int(community_id)
            pipeline1 = [
                {
                    u"$match": {
                        u"ComID": community_id,
                    }
                },
                {"$project":
                     {"ComID": community_id,
                      "result": {"$divide": [{"$multiply": [{"$sqrt": [{"$add": [{"$multiply": [
                          {"$subtract": ["$station_latitude", "$com_latitude"]},
                          {"$subtract": ["$station_latitude", "$com_latitude"]}]}, {"$multiply": [
                          {"$subtract": ["$station_longitude", "$com_longitude"]},
                          {"$subtract": ["$station_longitude", "$com_longitude"]}]}]}]}, 69]}, 6]},
                      "_id": 0
                      }
                 }
            ]

            by_community_id = []
            cursor1 = collection2.aggregate(pipeline1, allowDiskUse=False)
            try:
                for doc in cursor1:
                    by_community_id.append(doc)
                return jsonify(by_community_id)
            finally:
                client.close()
        # End Case 1 #

        # Case 2 - Find the crime intensity by police beats in a specific year
        if request.form['Submit'] == "FindBeatYear":
            query2 = request.form
            beat = query2['BeatNumber']
            beat = int(beat)
            year = query2['Year2']
            year = int(year)
            pipeline2 = [
                {
                    u"$match": {
                        u"Beat": beat,
                        u"Year": year
                    }
                },
                {
                    u"$group": {
                        u"_id": "$Primary_Type",
                        u"count": {
                            u"$sum": 1
                        }
                    }
                }
            ]
            beat_by_year = []
            cursor2 = collection.aggregate(pipeline2, allowDiskUse=False)
            try:
                for doc in cursor2:
                    beat_by_year.append(doc)
                return jsonify(beat_by_year)
            finally:
                client.close()
        # End Case 2 #

        # Case 3 - Find crime intensity by community area in a specific year
        if request.form['Submit'] == "FindCommunityAreaYear":
            query3 = request.form
            community_area = query3['CommunityArea']
            community_area = int(community_area)
            year3 = query3['Year3']
            year3 = int(year3)
            pipeline3 = [
                {
                    u"$match": {
                        u"Community Area": community_area,
                        u"Year": year3
                    }
                },
                {
                    u"$group": {
                        u"_id": "$Primary Type",
                        u"count": {
                            u"$sum": 1
                        }
                    }
                }
            ]
            com_area_by_year = []
            cursor3 = collection.aggregate(pipeline3, allowDiskUse=False)
            try:
                for doc in cursor3:
                    com_area_by_year.append(doc)
                return jsonify(com_area_by_year)
            finally:
                client.close()
        # End Case 3 #

        # Case 4 - Find the occurrences of a specific crime type in a particular year
        if request.form['Submit'] == "FindCrimeTypeYear":
            query4 = request.form
            crime_type4 = query4['CrimeType4']
            year4 = query4['Year4']
            year4 = int(year4)
            pipeline4 = [
                {
                    u"$match": {
                        u"Primary Type": crime_type4,
                        u"Year": year4
                    }
                },
                {
                    u"$group": {
                        u"_id": "$Primary Type",
                        u"count": {
                            u"$sum": 1
                        }
                    }
                }
            ]
            crime_type_by_year = []
            cursor4 = collection.aggregate(pipeline4, allowDiskUse=False)
            try:
                for doc in cursor4:
                    crime_type_by_year.append(doc)
                return jsonify(crime_type_by_year)
            finally:
                client.close()
        # End Case 4 #

        # Case 5 - Insert the reported incidents into the collection
        if request.form['Submit'] == 'Report':
            query5 = request.form
            year5 = query5['Year5']
            crime_type5 = query5['CrimeType5']
            description5 = query5['Description']
            collection.insert({
                "Year": year5,
                "Primary Type": crime_type5,
                "Description": description5
            })
            cursor5 = collection.find()
            return dumps(cursor5)
        # End Case 5 #


if __name__ == "__main__":
    CrimeApp.debug = True
    CrimeApp.run(host="localhost", port=8000, debug=True)
