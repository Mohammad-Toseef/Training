import os.path

from flask import Flask
from flask import send_file

app = Flask(__name__)

@app.route("/ebs")
def ebs():
    return send_file(os.path.join(os.path.dirname(__file__), f'efs-mount{os.sep}user_data.json'))
