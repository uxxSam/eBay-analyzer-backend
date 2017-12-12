from flask import Flask,jsonify
from flask_pymongo import PyMongo
from flask import request

import requests
import simplejson
import json

app = Flask(__name__)
mongo = PyMongo(app)

@app.route("/")
def home():
    fetchStatus = fetch_data()
    database_clean_up(request.args.get('item', default = "", type = str))
    return "Success!"

def fetch_data():
    urlPrefix = "http://svcs.ebay.com/services/search/FindingService/v1?OPERATION-NAME=findCompletedItems&SERVICE-VERSION=1.7.0&SECURITY-APPNAME="
    apiKey = "SamXu-EbayMark-PRD-67a8f6fb5-f4fa7d8b"
    urlMiddlefix = "&RESPONSE-DATA-FORMAT=JSON&REST-PAYLOAD&keywords="
    itemName = request.args.get('item', default = "", type = str)

    # check if input is empty
    if len(itemName) == 0:
        print(" *** System Message *** Error: No item name given in request")
        return 1

    print(" *** System Message *** Started data fetch for item named: " + itemName)
    urlPostfix = "&itemFilter(0).name=SoldItemsOnly&itemFilter(0).value=true&sortOrder=BestMatch&outputSelector=PictureURLSuperSize&paginationInput.pageNumber="
    pageNumber = 1
    loadedRecordCount = 0

    # start fetching data until hit last page (item < 100)
    while True:
        uri = urlPrefix + apiKey + urlMiddlefix + itemName + urlPostfix + str(pageNumber)

        # try get data from api
        try:
            Jresponse = requests.get(uri).text
        except requests.ConnectionError:
            print(" *** System Message *** Error: Connection Error")
            return 1
        # convert data to json and save to MongoDB
        data = json.loads(Jresponse)

        mongo.db[itemName].insert_many(data['findCompletedItemsResponse'][0]['searchResult'][0]['item'])
        loadedRecordCount += int(data['findCompletedItemsResponse'][0]['searchResult'][0]['@count'])
        pageNumber += 1
        # break if hit last page
        if data['findCompletedItemsResponse'][0]['searchResult'][0]['@count'] != '100':
            break

    print(" *** System Message *** Loaded " + str(loadedRecordCount) + " records")
    return 0

def database_clean_up(itemName):
    # remove the collection from database when finished
    if (mongo.db[itemName].count() != 0):
        print(" *** System Message *** Cleaning up...")
        mongo.db[itemName].drop()

if __name__ == "__main__":
    app.run(debug = True)
