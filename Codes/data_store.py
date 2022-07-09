import json
import time

import requests
import csv
from collecting_stocks import get_stock

url = "https://stock-market-data.p.rapidapi.com/stock/historical-prices"
count = 0
for stock in get_stock():
    if count < 25:
        querystring = {"ticker_symbol": stock, "years": "5", "format": "json"}
        headers = {
            "X-RapidAPI-Key": "4344730984msh7b2653ed51d067dp11b5cfjsn582e7922aaba",
            "X-RapidAPI-Host": "stock-market-data.p.rapidapi.com"
        }
        response = requests.request("GET", url, headers=headers, params=querystring)

        headers = ["Open", "High", "Low", "Close", "Adj_close", "Volume", "Date", "Stock_name"]
        json_response = json.loads(response.text)
        # stock_data = json_response['historical prices']
        # stock_data=stock

        # now we will open a file for writing
        data_file = open(f'New_data/{stock}.csv', 'w')

        # create the csv writer object
        csv_writer = csv.writer(data_file)

        rows = [headers]
        for data in json_response['historical prices']:
            temp = []
            temp = list(data.values())
            temp.append(stock)
            rows.append(temp)

            # # Writing data of CSV file
            # csv_writer.writerow(data.values())

        csv_writer.writerows(rows)

        data_file.close()
        count = count + 1
        time.sleep(8)

    # print(json_response)
