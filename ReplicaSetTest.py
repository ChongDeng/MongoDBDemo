#coding=utf-8

import pymongo
import pprint

client = pymongo.MongoClient('mongodb://192.168.1.88:27017,192.168.1.76:27017/?replicaSet=scut_cluster')

db = client.IfcAnalytics #数据库mydb
print(db.SystemInfo.count())
