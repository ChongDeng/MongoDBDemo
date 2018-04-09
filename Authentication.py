from pymongo import MongoClient

client = MongoClient('localhost', 27017)
db = client.IfcAnalytics
db.authenticate("fqyang", "gnayqf")

print(db.SystemInfo.count())
