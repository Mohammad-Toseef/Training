"""
This script sets the username and password for sql server in environment file
Note : Running this script only one time is required
"""
from database import Credentials
from getpass import getpass

username = input('Enter username for sql')
password = getpass('Enter Password: ')
cred_obj = Credentials(username=username, password=password)
cred_obj.set()
