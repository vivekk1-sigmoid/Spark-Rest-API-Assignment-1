import json

import requests

url = "https://stock-market-data.p.rapidapi.com/market/index/s-and-p-six-hundred"

headers = {
    "X-RapidAPI-Key": "4344730984msh7b2653ed51d067dp11b5cfjsn582e7922aaba",
    "X-RapidAPI-Host": "stock-market-data.p.rapidapi.com"
}
response = requests.request("GET", url, headers=headers)
json_response = json.loads(response.text)


# print(response.text)
# print(json_response)


def get_stock():
    total_stocks = json_response['stocks']
    stocks_100 = total_stocks[0:100]
    # file = open('stocks.csv', 'w+', newline='')
    #
    # # writing the data into the file
    # with file:
    #     write = csv.writer(file)
    #     write.writerows(stocks_100)
    return stocks_100
