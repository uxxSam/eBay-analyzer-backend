from flask import Flask,jsonify
from flask_pymongo import PyMongo
from flask import request

import timeit
import requests
import simplejson
import json

app = Flask(__name__)
mongo = PyMongo(app)

@app.route("/")

def home():
    startTime = timeit.default_timer()
    itemName = request.args.get('item', default = "", type = str)
    fetchStatus = fetch_data(itemName)
    res = process_data(itemName)
    database_clean_up(itemName)
    stopTime = timeit.default_timer()

    print(" *** System Message *** Request finished in " + str(int((stopTime - startTime) * 1000)) + " ms")
    return json.dumps(res)

def fetch_data(itemName):
    urlPrefix = "http://svcs.ebay.com/services/search/FindingService/v1?OPERATION-NAME=findCompletedItems&SERVICE-VERSION=1.7.0&SECURITY-APPNAME="
    apiKey = "SamXu-EbayMark-PRD-67a8f6fb5-f4fa7d8b"
    urlMiddlefix = "&RESPONSE-DATA-FORMAT=JSON&REST-PAYLOAD&keywords="

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

def process_data(itemName):
    data = list(mongo.db[itemName].find())
    priceSum = 0
    print(" *** System Message *** Start processing the data")
    # pre-process the data
    for listing in data:
        priceSum += (float)(listing['sellingStatus'][0]['convertedCurrentPrice'][0]['__value__'])
    priceFilterThreshold = 3
    # now can use this limit to filter out listings with unresonable prices
    filterLimit = priceSum / len(data) / priceFilterThreshold

    # actual analysis starts here
    actualListCount = 0
    actualListPriceSum = 0
    buyerPaysShipping = 0
    sellerPaysShipping = 0
    shippingCostSum = 0
    currLowestPriceIndex = 0
    currLowestPrice = 2147483647
    currHighestPriceIndex = 0
    currHighestPrice = -1
    monthlySold = [0] * 12
    monthlyPrice = [0] * 12
    monthlyAveragePrice = [0] * 12

    for i in range(0, len(data)):
        soldPrice = (float)(data[i]['sellingStatus'][0]['convertedCurrentPrice'][0]['__value__'])
        # if price is lower than limit, this listing will not be counted
        if soldPrice < filterLimit:
            continue
        actualListCount += 1
        actualListPriceSum += soldPrice

        # if shipping cost does not exist or == 0, it means free shipping
        if ('shippingServiceCost' in data[i]['shippingInfo'][0]):
            shippingCost = (float)(data[i]['shippingInfo'][0]['shippingServiceCost'][0]['__value__'])
            if shippingCost == 0:
                sellerPaysShipping += 1
            else:
                buyerPaysShipping += 1
                shippingCostSum += shippingCost
        else:
            sellerPaysShipping += 1

        # keep track on highest/lowest price listing so far
        if soldPrice > currHighestPrice:
            currHighestPrice = soldPrice
            currHighestPriceIndex = i
        if soldPrice < currLowestPrice:
            currLowestPrice = soldPrice
            currLowestPriceIndex = i

        # get the month the item was sold by splitting the end time
        soldMonth = int(data[i]['listingInfo'][0]['endTime'][0].split("-")[1])
        monthlySold[soldMonth - 1] += 1
        monthlyPrice[soldMonth - 1] += soldPrice

    for month in range(0, len(monthlyPrice)):
        monthlyAveragePrice[month] = 0 if monthlySold[month] == 0 else int(monthlyPrice[month] / monthlySold[month])

    analysisResult = {}
    analysisResult['ThreeMonthSold'] = actualListCount
    analysisResult['AveragePrice'] = int(actualListPriceSum / actualListCount)
    analysisResult['FreeShippingPercent'] = int((sellerPaysShipping / (sellerPaysShipping + buyerPaysShipping)) * 100)
    analysisResult['AverageShippingCost'] = int(shippingCostSum / buyerPaysShipping)
    analysisResult['monthlySold'] = monthlySold
    analysisResult['monthlyAveragePrice'] = monthlyAveragePrice

    return analysisResult

def database_clean_up(itemName):
    # remove the collection from database when finished
    if (mongo.db[itemName].count() != 0):
        print(" *** System Message *** Cleaning up...")
        mongo.db[itemName].drop()

if __name__ == "__main__":
    app.run(debug = True, threaded=True)
