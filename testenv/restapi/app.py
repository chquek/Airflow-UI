
from flask import Flask , request
from datetime import datetime
import socket

app = Flask(__name__)

@app.route("/" , methods = [ 'GET' , 'POST' ] )
def index():
    return "hello world\n"

@app.route("/dummy" , methods = [ 'GET' , 'POST'] )
def dummy():

    if request.method == 'POST':
        datain = request.json
        datain['host'] = socket.gethostname()
        datain['from'] = 'echo-from-dummy-post'
        return dict(ans=datain)

    if request.method == 'GET':
        print ("ARGS",request.args.to_dict())
        datain = request.args.to_dict()
        datain['host'] = socket.gethostname()
        datain['from'] = 'echo-from-dummy-get'
        return dict(ans=datain)


app.run ( "0.0.0.0" , 7654 )
