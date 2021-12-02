#requests module to send http request
import requests
#csv module to save output in csv format
import csv

#calling get method to receive the response from api
response = requests.get("https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=100&page=1&sparkline=true&price_change_percentage=1h%2C24h")
#Creating new csv file
file = open('C:/Users/Mohammad Touseef/Documents/Output.csv','w',newline='')
csv_writer = csv.writer(file)
line = 0
arr = []
#Looping through each item in output
for item in response.json():
    if line == 0:
        #writing column names on 1st row
        csv_writer.writerow(item.keys())
    else:
        #writing values corresponding to columns
        csv_writer.writerow(item.values())
    line+=1
file.close()