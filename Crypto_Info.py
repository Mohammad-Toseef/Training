#LIBRARY TO GET REQUEST FROM SERVERS
import requests
#LIBRARY TO SAVE THE DATA IN CSV FORMAT
import csv
# Documentation is in: https://www.coingecko.com/en/api/documentation
headers = {
    'Accept': 'application/json',
    'Content-Type': 'application/json',
}
#Requesting server to get the details and storing response in result variable
response = requests.get(
    "https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=100&page=1&sparkline=false&price_change_percentage=1h%2C24h", headers=headers)
#Creating a CSV file using open() method in F: Drive
file = open('C:/output.csv','w',newline = '')
csv_writer = csv.writer(file)
line = 0
#reading data and storing it in csv file
for item in response.json():
    if line==0:
        csv_writer.writerow(item.keys())
    else:
        csv_writer.writerow(item.values())
    line+=1
#printing to console
[print(x) for x in response.json()]
file.close()