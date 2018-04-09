from io import BytesIO

import bson
from pymongo import MongoClient
from bson.objectid import ObjectId
from gridfs import *

def InsertPic():
    client = MongoClient('localhost', 27017)
    db = client.ScutBinaryFile
    coll = db.image

    filename = 'C:/Users/fqyya/Desktop/qq.jpg'
    with open(filename, 'rb') as myimage:
        content = BytesIO(myimage.read())
        coll.save(dict(
            content=bson.binary.Binary(content.getvalue()),
            filename='scut_qq.jpg'
        ))


def getPic():
    client = MongoClient('localhost', 27017)
    db = client.ScutBinaryFile
    coll = db.image

    data = coll.find_one({'filename': 'scut_qq.jpg'})
    out = open('C:/Users/fqyya/Desktop/qq_2.jpg', 'wb')
    out.write(data['content'])
    out.close()


##############  following is for grid fs

def GridFsInsertFile():
    client = MongoClient('localhost', 27017)
    db = client.pic
    fs = GridFS(db, 'images')

    with open ('C:/Users/fqyya/Desktop/qq.jpg','rb') as myimage:
        data=myimage.read()
        id = fs.put(data,filename='first')
        print(id)

def GridFsGetFile():
    client = MongoClient('localhost', 27017)
    db = client.pic
    fs = GridFS(db, 'images')
    file = fs.get_version('first', 0)
    data = file.read()
    out = open('C:/Users/fqyya/Desktop/qq_3.jpg','wb')
    out.write(data)
    out.close()

def GridFsDelFile():
    client = MongoClient('localhost', 27017)
    db = client.Pic
    fs = GridFS(db, 'images')
    fs.delete(ObjectId('560a531b0d4eae34a4edbfdd'))

def GridFsListName():
    client = MongoClient('localhost', 27017)
    db = client.pic
    fs = GridFS(db, 'images')
    print(fs.list())





def main():
    print("hello" + "world")
    # InsertPic()
    # getPic()
    # GridFsInsertFile()
    # GridFsGetFile()
    GridFsListName()



if __name__ == '__main__':
    main()