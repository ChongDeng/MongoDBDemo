from flask import Flask
import socket, datetime
from pymongo import MongoClient

app = Flask(__name__)

#step1: 创建客户端
#client = MongoClient()
#The above code will connect on the default host and port.
# We can also specify the host and port explicitly, as follows:
client = MongoClient('localhost', 27017)

#step2: 获得数据库 Getting a Database
#db = client.mongodb_tutorial

database_name = "department"
db = client[database_name]
db.authenticate("fqyang", "123")

#step3: 获得数据表student
#students = db.student
students = db['student']


@app.route('/')
def hello():
    HostName = socket.gethostname()
    HostIP = socket.gethostbyname(HostName)
    return "Hello! My Hostname is " + HostName + ". My IP is " + HostIP


@app.route('/info')
def info():
    #return 'hello'
    obj = students.find_one()
    return obj['name']

@app.route('/access')
def access():
    HostName = socket.gethostname()
    HostIP = socket.gethostbyname(HostName)
    item = {"IP": HostIP, "DateTime" : datetime.datetime.utcnow().strftime('%Y-%m-%d-%H_%M_%S')}
    # even if collection does not exitst, we still can insert
    students.insert(item)
    return 'access has been recorded'

if __name__ == "__main__":
    app.run(host='0.0.0.0', port = 8086, debug = True)
    #app.run(host='0.0.0.0', port = 8086)
