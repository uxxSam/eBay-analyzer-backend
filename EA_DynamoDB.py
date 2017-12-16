from flask import Flask,jsonify
from flask import request
from flask_cors import CORS

import concurrent.futures
import timeit
import requests
import simplejson
import json
import boto3

app = Flask(__name__)
CORS(app)

dynamodb = boto3.resource('dynamodb',
                          aws_access_key_id="anything",
                          aws_secret_access_key="anything",
                          region_name="us-west-2",
                          endpoint_url="http://localhost:8000")

dbIndex = 0

@app.route("/")

def home():
    # reset dbIndex when starts
    global dbIndex
    dbIndex = 0

    startTime = timeit.default_timer()
    itemName = request.args.get('item', default = "", type = str)
    tableName = fetch_data(itemName)
    # test_db(tableName)
    res = process_data(tableName)
    database_clean_up(tableName)
    stopTime = timeit.default_timer()

    print(" *** System Message *** Request finished in " + str(int((stopTime - startTime) * 1000)) + " ms")
    return json.dumps(res)

def fetch_one(url,app,table):
    with app.app_context():

        try:
            Jresponse = requests.get(url).text
        except requests.ConnectionError:
            print(" *** System Message *** Error: Connection Error")

        # convert data
        data = json.loads(Jresponse)
        for listing in data['findCompletedItemsResponse'][0]['searchResult'][0]['item']:

            # assign dbIndex as primary key to added each item to Dynamodb
            global dbIndex
            dbIndex += 1
            listing['id'] = dbIndex
            table.put_item(Item = listing)

def get_total_page(url):

    try:
        Jresponse = requests.get(url + str(1)).text
    except requests.ConnectionError:
        print(" *** System Message *** Error: Connection Error")

    data = json.loads(Jresponse)
    return int(data['findCompletedItemsResponse'][0]['paginationOutput'][0]['totalPages'][0]) + 1

def fetch_data(itemName):
    urlPrefix = "http://svcs.ebay.com/services/search/FindingService/v1?OPERATION-NAME=findCompletedItems&SERVICE-VERSION=1.7.0&SECURITY-APPNAME="
    apiKey = "SamXu-EbayMark-PRD-67a8f6fb5-f4fa7d8b"
    urlMiddlefix = "&RESPONSE-DATA-FORMAT=JSON&REST-PAYLOAD&keywords="
    urlPostfix = "&itemFilter(0).name=SoldItemsOnly&itemFilter(0).value=true&sortOrder=BestMatch&outputSelector=PictureURLSuperSize&paginationInput.pageNumber="
    urlBeforePage = urlPrefix + apiKey + urlMiddlefix + itemName + urlPostfix

    # first get one page of data to see how many pages exist
    totalPage = get_total_page(urlBeforePage)

    # generate urls to be fetched
    urls = []
    for i in range(1, totalPage):
        urls.append(urlBeforePage + str(i))

    tableNameList = []

    # sanitize of input
    for c in itemName:
        if (c >= 'a' and c <= 'z') or (c >= 'A' and c <= 'Z') or (c >= '0' and c <= '9') or c == '_' or c == '-' or c == '.':
            tableNameList.append(c)
    tableName = ''.join(tableNameList)

    table = dynamodb.create_table(
        TableName=tableName,
        KeySchema=[
            {
                'AttributeName': 'id',
                'KeyType': 'HASH'  #Partition key
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'id',
                'AttributeType': 'N'
            }
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 1,
            'WriteCapacityUnits': 1
        }
    )

    # fetch data with multiple threads
    with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
        future_to_url = {executor.submit(fetch_one, url, app, table): url for url in urls}

    # finished fetching, return the tableName for next step
    return tableName

def test_db(tableName):
    table = dynamodb.Table(tableName)
    data = table.scan()
    for subData in data['Items']:
        print(subData)

def process_data(tableName):
    table = dynamodb.Table(tableName)
    data = table.scan()['Items']
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

    # include info for lowest and highest price item
    analysisResult['lowestItemPrice'] = data[currLowestPriceIndex]['sellingStatus'][0]['convertedCurrentPrice'][0]['__value__']
    analysisResult['lowestItemPic'] = "" if 'pictureURLSuperSize' not in data[currLowestPriceIndex] else data[currLowestPriceIndex]['pictureURLSuperSize'][0]
    analysisResult['lowestItemName'] = data[currLowestPriceIndex]['title'][0]
    analysisResult['lowestItemDate'] = data[currLowestPriceIndex]['listingInfo'][0]['endTime'][0][:10]
    analysisResult['lowestItemUrl'] = data[currLowestPriceIndex]['viewItemURL'][0]
    analysisResult['lowestItemBids'] = 0 if 'bidCount' not in data[currLowestPriceIndex]['sellingStatus'][0] else data[currLowestPriceIndex]['sellingStatus'][0]['bidCount'][0]

    analysisResult['highestItemPrice'] = data[currHighestPriceIndex]['sellingStatus'][0]['convertedCurrentPrice'][0]['__value__']
    analysisResult['highestItemPic'] = "" if 'pictureURLSuperSize' not in data[currHighestPriceIndex] else data[currHighestPriceIndex]['pictureURLSuperSize'][0]
    analysisResult['highestItemName'] = data[currHighestPriceIndex]['title'][0]
    analysisResult['highestItemDate'] = data[currHighestPriceIndex]['listingInfo'][0]['endTime'][0][:10]
    analysisResult['highestItemUrl'] = data[currHighestPriceIndex]['viewItemURL'][0]
    analysisResult['highestItemBids'] = 0 if 'bidCount' not in data[currHighestPriceIndex]['sellingStatus'][0] else data[currHighestPriceIndex]['sellingStatus'][0]['bidCount'][0]

    return analysisResult

def database_clean_up(tableName):
    # remove the collection from database when finished
    table = dynamodb.Table(tableName)
    table.delete()

if __name__ == "__main__":
    app.run(debug = True, threaded=True)
