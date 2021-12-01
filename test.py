#LIBRARY TO GET REQUEST FROM SERVERS
import requests
#LIBRARY TO SAVE THE DATA IN CSV FORMAT
import csv
# Documentation is in: https://www.coingecko.com/en/api/documentation
#Header dictionary to specify type of data
headers = {
    'Accept': 'application/json',
    'Content-Type': 'application/json',
}
#Requesting server to get the details and storing it in result variable
result = requests.get(
    "https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=100&page=1&sparkline=false&price_change_percentage=1h%2C24h", headers=headers)
#Creating a CSV file using open() method in F: Drive
file = open('F:/output.csv','w')

#Splitting the data on the basis of comma which is returned by server
for data in result.text.split(','):
    for item in data.split('\n'):
        #Writing the data in csv file
        file.write(item)
    file.write('\n')
    print(data)
#Closing the csv file
file.close()