"""
CRUD Application using FLASK framework .
Author - Mohammad Toseef
"""
import os
import csv
import requests
import json
from s3_read_write import S3Operations
from flask import Flask, render_template,redirect
from flask import request, flash

from database import Connection

app = Flask(__name__)
app.secret_key = "flash message"
TABLE_NAME = ''
FILE_NAME = ''
connection = Connection()
connection.connect()
api_create_endpoint = 'https://usq56k2ezd.execute-api.ap-south-1.amazonaws.com/test/create'
api_insert_endpoint = 'https://usq56k2ezd.execute-api.ap-south-1.amazonaws.com/test/insert'
api_read_endpoint = 'https://usq56k2ezd.execute-api.ap-south-1.amazonaws.com/test/read'
api_delete_endpoint = 'https://usq56k2ezd.execute-api.ap-south-1.amazonaws.com/test/delete'
api_backup_endpoint = 'https://usq56k2ezd.execute-api.ap-south-1.amazonaws.com/test/backup'
query = f"SELECT * FROM {Connection.database}.{Connection.table_name}"

@app.route("/")
def index():
    """
    :return: output the homepage while server is running
    """

    return render_template("home.html")


@app.route("/upload_file", methods=['POST'])
def upload_file():
    """
    Uploads file to the database
    :return: Success page
    """
    file = request.files['filename']
    app.FILE_NAME = connection.file_name = file.filename
    file.save(os.path.join(os.path.dirname(__file__), file.filename))
    app.TABLE_NAME = "_".join(file.filename.split('.'))
#    connection.upload_file(file.filename)
    with open(os.path.join(os.path.dirname(__file__), file.filename), 'r',
              encoding="utf-8") as file_obj:
        reader = csv.reader(file_obj)
        create_statement = Connection.create_statement(reader)
        requests.post(url=f'{api_create_endpoint}?operation=create&table={app.TABLE_NAME}', data=create_statement)
    with open(os.path.join(os.path.dirname(__file__), file.filename), 'r',
              encoding="utf-8") as file_obj:
        reader = csv.reader(file_obj)
        # stores insert statement
        insert_statement = Connection.insert_statement(reader)
        requests.post(url=f'{api_insert_endpoint}?operation=insert&table={app.TABLE_NAME}', data=insert_statement)
    data = requests.get(url=f'{api_read_endpoint}?operation=read&table={app.TABLE_NAME}&query={query}')

    return render_template("Success.html", data=json.loads(data.text), msg='File uploaded Successfully')


@app.route("/add", methods=['POST', 'GET'])
def add():
    """
    renders add.html on Get request ,
    renders Success.html when row_data has been added on post request
    renders add.html with error message when row_data is not proper
    """
    columns = connection.table_columns
    if request.method == 'POST':
        insert = connection.add_row(request.form.to_dict())
        requests.post(url=f'{api_insert_endpoint}?operation=insert&table={app.TABLE_NAME}', data=insert)
        # if message:
        #     flash(message, 'error')
        #return render_template("add.html", columns=columns)
        data = requests.get(url=f'{api_read_endpoint}?operation=read&table={app.TABLE_NAME}&query={query}')
        return render_template("Success.html", data=json.loads(data.text), msg='Record added Successfully')
    return render_template("add.html", columns=columns)


@app.route('/update/<record_id>', methods=['POST', 'GET'])
def update(record_id):
    """
    GET request : renders edit page
    POST request : makes record update request to database and renders Success.html

    :param record_id: unique id of the record to be updated
    :return: GET - edit.html / POST - Success.html

    """
    if request.method == 'POST':
        # Connection.update(request.form.to_dict(), record_id)
        # data = connection.select_statement()
        update_query = f'UPDATE {Connection.database}.{Connection.table_name} SET '
        statement = ''
        for key, value in request.form.to_dict().items():
            statement += str(key) + ' = \'' + str(value) + '\' ,'
        update_query = update_query + statement[:-1] + f" WHERE id = '{str(record_id)}'"
        requests.post(url=f'{api_insert_endpoint}?operation=insert&table={app.TABLE_NAME}', data=update_query)
        data = requests.get(url=f'{api_read_endpoint}?operation=read&table={app.TABLE_NAME}&query={query}')
        return render_template("Success.html", data=json.loads(data.text), msg='Record Updated Successfully')
    conditional_query = f"{query} WHERE id='{record_id}'"
    # data = connection.select_statement(record_id)
    data = requests.get(url=f'{api_read_endpoint}?operation=read&table={app.TABLE_NAME}&query={conditional_query}')
    return render_template("edit.html", data=json.loads(data.text)[1], headers=Connection.table_columns)


@app.route('/delete/<record_id>')
def delete(record_id):
    """
    deletes record for the given id from DB
    :param record_id: id that recognize the row uniquely
    :return: renders Success.html
    """
#    data, msg = connection.delete_row(record_id)
    msg = requests.delete(url=f'{api_delete_endpoint}?operation=delete&table={app.TABLE_NAME}&record_id={str(record_id)}')
    data = requests.get(url=f'{api_read_endpoint}?operation=read&table={app.TABLE_NAME}&query={query}')
    return render_template("Success.html", data=json.loads(data.text), msg=msg.text)


@app.route('/backup')
def backup():
    # query_result = pd.read_sql_query(f"SELECT * FROM {Connection.database}.{Connection.table_name}", Connection.my_db)
    # df = pd.DataFrame(query_result)
    # print(f"file name is {connection.file_name}")
    # df.to_csv(os.path.join(os.path.dirname(__file__), connection.file_name), index=False)
    # bucket_name = 'first-ui-bucket'
    # s3_obj = S3Operations(bucket_name)
    # s3_obj.write_s3object(connection.file_name, connection.file_name)
    msg = requests.post(url=f'{api_backup_endpoint}?operation=backup&table={app.TABLE_NAME}&file_name={app.FILE_NAME}')
    data = requests.get(url=f'{api_read_endpoint}?operation=read&table={app.TABLE_NAME}&query={query}')
    return render_template("Success.html", data=json.loads(data.text), msg=msg.text)


@app.route('/restore')
def restore():
    bucket_name = 'first-ui-bucket'
    s3_obj = S3Operations(bucket_name)
    s3_obj.read_s3object(connection.file_name)
    with open(os.path.join(os.path.dirname(__file__), app.FILE_NAME), 'r',
              encoding="utf-8") as file_obj:
        reader = csv.reader(file_obj)
        create_statement = Connection.create_statement(reader)
        requests.post(url=f'{api_create_endpoint}?operation=create&table={app.TABLE_NAME}', data=create_statement)
    with open(os.path.join(os.path.dirname(__file__), app.FILE_NAME), 'r',
              encoding="utf-8") as file_obj:
        reader = csv.reader(file_obj)
        # stores insert statement
        insert_statement = Connection.insert_statement(reader)
        requests.post(url=f'{api_insert_endpoint}?operation=insert&table={app.TABLE_NAME}', data=insert_statement)
    data = requests.get(url=f'{api_read_endpoint}?operation=read&table={app.TABLE_NAME}&query={query}')

    return render_template("Success.html", data=json.loads(data.text), msg='File uploaded Successfully')


@app.route('/shutdown')
def shutdown():
    request.data
    func = request.environ.get('werkzeug.server.shutdown')
    func()
    return "Quitting..."


app.debug = True

if __name__ == "__main__":
    app.run(port=5000)
