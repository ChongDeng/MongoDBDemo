#-*-coding:utf8-*-
import datetime

from copy import deepcopy

from bson import ObjectId, SON, Code
from pymongo import MongoClient, ASCENDING

#step1: 创建客户端
#client = MongoClient()
#The above code will connect on the default host and port.
# We can also specify the host and port explicitly, as follows:
client = MongoClient('localhost', 27017)

#step2: 获得数据库 Getting a Database
#db = client.mongodb_tutorial


database_name = "mongodb_tutorial"
db = client[database_name]

# database_name = "IfcAnalytics"
# db = client[database_name]

#step3: 获得数据表 Getting a posts
#A posts is a group of documents stored in MongoDB, and can be thought of as
#roughly the equivalent of a table in a relational database.

#posts = db.test_posts
#posts = db['test_posts']

# table_name = "user"
# muser = db[table_name]
#
# muser.remove()
# muser.save({'id':1, 'name':'test1'}) # add a record
# muser.save({'id':2, 'name':'test2'}) # add a record
# muser.save({'id':2, 'name':'test2'}) # add a record
# muser.save({'id':2, 'name':'test2'}) # add a record
# muser.save({'id':3, 'name':'test3'}) # add a record
#
# print(muser.count())
# muser.remove({'id' : 2})
# for post in muser.find():
#     print(post)

def insert_data():
    demo1 = {"x": 1}
    #even if demo collection does not exitst, we still can insert
    db['demo'].insert(demo1)


def insert_many():
    docs = [
        {"_id": ObjectId("512bc95fe835e68f199c8686"), "author": "dave", "score": 80, "views": 100},
        {"_id": ObjectId("512bc962e835e68f199c8687"), "author": "dave", "score": 85, "views": 521},
        {"_id": ObjectId("55f5a192d4bede9ac365b257"), "author": "ahn", "score": 60, "views": 1000},
        {"_id": ObjectId("55f5a192d4bede9ac365b258"), "author": "li", "score": 55, "views": 5000},
        {"_id": ObjectId("55f5a1d3d4bede9ac365b259"), "author": "annT", "score": 60, "views": 50},
        {"_id": ObjectId("55f5a1d3d4bede9ac365b25a"), "author": "li", "score": 94, "views": 999},
        {"_id": ObjectId("55f5a1d3d4bede9ac365b25b"), "author": "ty", "score": 95, "views": 1000}
    ]
    db.articles.insert_many(docs)

def insert_many2():
        docs = [
            {"_id": 1, "item": "abc", "price": 10, "fee": 2, "date": "2014-03-01T08:00:00Z"},
            {"_id": 2, "item": "jkl", "price": 20, "fee": 1, "date": "2014-03-01T09:00:00Z"},
            {"_id": 3, "item": "xyz", "price": 5, "fee": 0, "date": "2014-03-15T09:00:00Z"}

        ]
        db.sales.insert_many(docs)

def insert_many3():
    docs = [
        {"_id": 1, "item": "abc1", "description": "product 1", "qty": 300},
        {"_id": 2, "item": "abc2", "description": "product 2", "qty": 200},
        {"_id": 3, "item": "xyz1", "description": "product 3", "qty": 250},
        {"_id": 4, "item": "VWZ1", "description": "product 4", "qty": 300},
        {"_id": 5, "item": "VWZ2", "description": "product 5", "qty": 180}

    ]
    db.inventory.insert_many(docs)


def insert_2_nonexistent_collection():
    doc1 = {"x": 1}
    #if new_collection collection does not exitst: create it, then insert doc1
    db['new_collection'].insert(doc1)

def typeConversion():
    pass
    #demo2 = {"x": NumberInt("2")}  cant do that: that's js syntax
    #db['demo'].insert(demo2)

def query_data():
    # print(db['demo'].find())  wrong method: return a cursor
    print(db['demo'].find_one())
    print(db.demo.find_one())  #way 2 to use collection
    TableName = "demo"
    print(db[TableName].find_one())  # way 3 to use collection

def query_with_multiple_cond():
    #method 1:
    res = db.inventory.find({
        "$and":
            [{"qty": 300},
             {"item": "abc1"}]
    })
    for doc in res:
        print(doc)

    # method 2:
    res = db.inventory.find({"qty" : 300, "item": "abc1"})
    for doc in res :
        print(doc)


def query_expression():
    # print(db.demo.find_one({"y": "/ABC/i"})) wrong method
    import re
    regx = re.compile("^ABC", re.IGNORECASE)
    print(db.demo.find_one({"y": regx}))

def array_with_different_elem_types():
    doc1 = {"x": [1, "2", 3.4]}
    #even if demo collection does not exitst, we still can insert
    db['demo'].insert(doc1)
    print(db.demo.find_one(doc1))

def array_in_array():
    doc1 = {"x": [1, "2", 3.4, [1, "2", 3.4] ]}
    #even if demo collection does not exitst, we still can insert
    db['demo'].insert(doc1)
    print(db.demo.find_one(doc1))

def remove_all():
    docs = [
        {"x": 1, "_id" : 1},
        {"x": 2, "_id": 2},
        {"x": 3, "_id": 3}
    ]
    res = db.remove_test.insert_many(docs)
    print(res.inserted_ids)

    db.remove_test.remove()

    print(db.remove_test.count())

def update_demo1():
    doc = {
        "_id" : 1,
        "name": "scut",
        "friends": 32
    }
    db.remove_test.insert(doc)

    res = db.remove_test.find_one({"name": "scut"})
    new_doc = {
        "_id": res["_id"],
        "name": "scut2",
        "friends": res["friends"] + 3
    }
    db.remove_test.update(doc, new_doc)

def update_by_inc():
    doc = {
        "_id": 1,
        "name": "scut",
        "friends": 32
    }
    db.remove_test.update({"name": "scut"},
                          {"$inc": {"friends" : 1}})

def unset_demo():
    db.remove_test.update({"name": "scut"},
                          {"$unset": {"friends": 1}})

def array_push_demo():
    print(db.demo.find_one())
    db.demo.update({'x': 1},
                   {"$push":
                        {"comments": {"name": 'tom', "content" : "hello world"}}
                    })
    print(db.demo.find_one())


def array_push_each_demo():
    db.demo2.insert({"x" : 1})
    print(db.demo2.find_one())
    db.demo2.update({'x': 1},
                   {"$push":
                        {"comments": { "$each": ["good", "so so", "bad"]}}
                    })
    print(db.demo2.find_one())

def array_push_slice_demo():
    db.demo2.remove()
    db.demo2.insert({"x" : 1})
    print(db.demo2.find_one())
    db.demo2.update({'x': 1},
                    {"$push":
                        {"top10":
                             {
                                 "$each": [1,2,3,4,5,6,7],
                                 "$slice" : -5
                             }
                        }
                    })
    print(db.demo2.find_one())

def array_as_set_demo():
    db.demo2.remove()
    db.demo2.insert({"x": 1})
    print(db.demo2.find_one())
    db.demo2.update({'x': 1},
                    {"$push":
                         {"comments": {"$each": ["good", "so so", "bad"]}}
                     })
    print(db.demo2.find_one())

    db.demo2.update({"comments":{"$ne" : "shit"}},
                    {"$push":
                         {"comments": "shit"}
                    })
    print(db.demo2.find_one())

def array_addToSet_demo():
    db.demo2.remove()
    db.demo2.insert({"x": 1})
    print(db.demo2.find_one())
    db.demo2.update({'x': 1},
                    {"$push":
                         {"comments": "good"}
                     })
    print(db.demo2.find_one())

    db.demo2.update({'x': 1},
                    {"$addToSet":
                         {"comments": "good"}
                    })
    print(db.demo2.find_one())

    db.demo2.update({'x': 1},
                    {"$addToSet":
                         {"comments": "bad"}
                     })
    print(db.demo2.find_one())


def array_remove_demo():
    db.demo2.remove()
    db.demo2.insert({"x": 1})
    db.demo2.update({'x': 1},
                    {"$push":
                        {"comments": { "$each": ["good", "so so", "bad"]}}
                    })
    print(db.demo2.find_one())

    db.demo2.update({'x': 1},
                    {"$pop":
                         {"comments": 1}
                    })
    print(db.demo2.find_one())

    db.demo2.update({'x': 1},
                    {"$pop":
                         {"comments": -1}
                     })
    print(db.demo2.find_one())

def array_remove_demo2():
    db.demo2.remove()
    db.demo2.insert({"x": 1})
    db.demo2.update({'x': 1},
                    {"$push":
                        {"comments": { "$each": ["good", "so so", "bad"]}}
                    })
    print(db.demo2.find_one())

    db.demo2.update({'x': 1},
                    {"$pull":
                         {"comments": "so so"}
                    })
    print(db.demo2.find_one())

def create_posts():
    post = {"author" : "lily",
             "contents" : "watch movive",
             "comments" : [
                 {
                     "comment" : "good",
                     "author" : "chao",
                     "votes" : 0
                 },
                 {
                     "comment": "so so",
                     "author": "juzhang",
                     "votes": 3
                 },
                 {
                     "comment": "bad",
                     "author": "yong",
                     "votes": -1
                 },
             ]
            }
    db.posts.insert(post)

def array_position_modifier_demo():
    print(db.posts.find_one())

    db.posts.update({'author': "lily"},
                    {"$inc":
                         {"comments.0.votes": 1}
                    })
    print(db.posts.find_one())

def array_position_modifier_demo2():
    print(db.posts.find_one())

    db.posts.update({'comments.author': "chao"},
                    {"$set":
                         {"comments.$.author": "diaoge"}
                    })
    print(db.posts.find_one())


def array_position_modifier_demo2():
    res = db.posts.find()
    print(res.count())
    print(type(res))
    # while res.hasNext():  wrong method: this is js syntax
    #     post = res.next()
    #     print(post)
    NewPosts = []
    for post in res:
        # NewPost = post  wrong: shallow copy
        NewPost = deepcopy(post)
        for index in range(len(NewPost["comments"])):
            if NewPost["comments"][index]["author"] == "chao":
                NewPost["comments"][index]["author"] = "tom"
        db.posts.update(post, NewPost)
        NewPosts.append(NewPost)


def array_position_modifier_demo2_2():
    res = db.posts.find()
    print(res.count())
    print(type(res))

    db.posts.update(
        {},
        { "$set": {"comments.$[].author": "tom"}}
    )

def upsert_demo():
    res = db.posts2.find()
    print(res.count())
    print(type(res))

    db.posts2.update(
        {"url" : "/blog"},
        { "$inc": {"pageviews": 1}},
        True
    )


def or_query():
    res = db.demo.find(
        {"$or" :
             [
                 {"x" : 1},
                 {"y": "ABc"}
             ]
        })

    print(res.count())

def in_query():
    res = db.user.find(
        {
            "name" :{"$in" : ["test1", "test3"]}
        }
    )

    print(res.count())

def not_query():
    res = db.user.find(
        {
            "name" :{
                        "$not" :
                            {"$in" : ["test1", "test3"]}
                    }
        }
    )

    print(res.count())

def null_demo():
    res = db.posts.find_one()
    print(res)

    res = db.posts.find(
        {"x" : None}
    )
    print(res.count())

    res = db.posts.find(
        {
            "x": {"$in" : [None], "$exists" : True}
        }
    )
    print(res.count())

def array_find_demo():
    db.posts.insert({"fruit": ["apple", "banana", "peach"]})
    res = db.posts.find_one({"fruit" : ["apple", "banana", "peach"]})
    print(res)
    db.posts.insert({"fruit": ["peach", "banana","apple"]})
    res = db.posts.find({"fruit": ["apple", "banana", "peach"]})
    print("exact match:", res.count())

    res = db.posts.find({"fruit": "apple"})
    print("unexact match:", res.count())

def array_all_find_demo():

    res = db.posts.find(
        {"fruit" : {"$all" : ["apple", "banana"]}}
    )

    for doc in res:
        print(doc)

def array_position_find_demo():

    res = db.posts.find(
        {"fruit" : {"$all" : ["apple", "banana"]}}
    )
    for doc in res:
        print(doc)

    print("=========================")
    res = db.posts.find(
        {"fruit.0": "apple"}
    )
    for doc in res:
        print(doc)

def array_size_find_demo():

    res = db.posts.find(
        {"fruit" : {"$all" : ["apple", "banana"]}}
    )
    for doc in res:
        print(doc)

    print("=========================")
    res = db.posts.find(
        {"fruit": {"$size" : 3}}
    )
    for doc in res:
        print(doc)

def array_size_find_demo2():

    res = db.posts.find(
        {"fruit" : {"$all" : ["apple", "banana"]}}
    )
    for doc in res:
        print(doc)

    print("=========================")
    res = db.posts.find(
        {"fruit": {"$size" : 3}}
    )
    for doc in res:
        print(doc)

def runCommand_demo():
    print(db.command("collstats", "posts"))

def match_demo():
    res = db.articles.aggregate(
        [{ "$match": {"author": "dave"}}]
    )

    for doc in res:
        print(doc)

def add_demo():
    res = db.sales.aggregate(
        [
            {
                "$project": {"item": 1, "total": { "$add": ["$price", "$fee"]}}
            }
        ]
    )

    for doc in res:
        print(doc)

def cmp_demo():
    res = db.inventory.aggregate(
       [
         {
           "$project":
              {
                "item": 1,
                "qty": 1,
                "cmpTo250": { "$cmp": [ "$qty", 250 ] },
                "_id": 0
              }
         }
       ]
    )

    for doc in res:
        print(doc)

def get_sum0(SerialNumber, DayTime):
    pipe = [
        {
            "$match": {"Attributes.SerialID": SerialNumber, "TimeStamp": DayTime}
        },
        {
            "$group": {
                "_id": None,
                "DurationSum": {"$sum": "$Attributes.Duration"}
            }
        }
    ]

    res = db.VideoMeeting.aggregate(
                [
                    {
                        "$match":{"Attributes.SerialID": SerialNumber, "TimeStamp": DayTime}
                    },
                    {
                        "$group": {
                            "_id": None,
                            "DurationSum": {"$sum": "$Attributes.Duration"}
                        }
                    }
                ]
            )

    print("shit: ", res)


def get_sum(SerialNumber, DayTime):
    pipe = [
        {
            "$match": {"Attributes.SerialID": SerialNumber, "TimeStamp": DayTime}
        },
        {
            "$group": {
                "_id": None,
                "DurationSum": {"$sum": "$Attributes.Duration"}
            }
        }
    ]
    res = db.VideoMeeting.aggregate(pipeline=pipe)

    print("res: ", list(res)[0]["DurationSum"])

def insert2():
    result = db.things.insert_many([{"x": 1, "tags": ["dog", "cat"]},
                                    {"x": 2, "tags": ["cat"]},
                                    {"x": 2, "tags": ["mouse", "cat", "dog"]},
                                    {"x": 3, "tags": []}])

def aggregate_demo1():
    import pprint
    pprint.pprint(list(db.things.find()))

    pipeline = [
                 {"$unwind": "$tags"},
                 {"$group": {"_id": "$tags", "count": {"$sum": 1}}},
                 {"$sort": SON([("count", -1), ("_id", -1)])}
                ]

    pprint.pprint(list(db.things.aggregate(pipeline)))

def map_reducer_demo():
    mapper = Code("""
                   function () {
                     this.tags.forEach(function(z) {
                       emit(z, 1);
                     });
                   }
                   """)

    reducer = Code("""
                    function (key, values) {
                      var total = 0;
                      for (var i = 0; i < values.length; i++) {
                        total += values[i];
                      }
                      return total;
                    }
                    """)
    result = db.things.map_reduce(mapper, reducer, "myresults")
    for doc in result.find():
        print(doc)

# has some bug !!!!
def map_reducer_demo2():
    mapper = Code("""
                   function () {
                     this.tags.forEach(function(z) {
                       emit(z, {'scut_count': 1});
                     });
                   }
                   """)

    reducer = Code("""
                    function (key, values) {
                      var total = 0;
                      for (var i = 0; i < values.length; i++) {
                        total += values[i].scut_count;
                      }
                      return {'my_count': total};
                    }
                    """)
    result = db.things.map_reduce(mapper, reducer, "myresults2")
    for doc in result.find():
        print(doc)

def map_reducer_cond():
    mapper = Code("""
                   function () {
                     this.tags.forEach(function(z) {
                       emit(z, 1);
                     });
                   }
                   """)

    reducer = Code("""
                    function (key, values) {
                      var total = 0;
                      for (var i = 0; i < values.length; i++) {
                        total += values[i];
                      }
                      return total;
                    }
                    """)
    result = db.things.map_reduce(mapper, reducer, "myresults", query={"x": {"$lt": 2}})
    for doc in result.find():
        print(doc)

# still has bug: cant output
def save_map_reduce_result():
    mapper = Code("""
                   function () {
                     this.tags.forEach(function(z) {
                       emit(z, 1);
                     });
                   }
                   """)

    reducer = Code("""
                    function (key, values) {
                      var total = 0;
                      for (var i = 0; i < values.length; i++) {
                        total += values[i];
                      }
                      return total;
                    }
                    """)
    result = db.things.map_reduce(mapper, reducer, "myresults3",
                                 {"replace": "scut_col", "db":"mongotest"})
                                  # SON([("replace", "scut_col"), ("db", "mongotest")]))


def main():
    print("hello" + "world")
    #insert_data()
    #insert_many()
    # insert_many2()
    # insert_many3()
    #query_data()
    # query_with_multiple_cond()
    # query_expression()
    #array_with_different_elem_types()
    #array_in_array()
    #insert_2_nonexistent_collection()
    # remove_all()
    #update_demo1()
    #update_by_inc()
    #unset_demo()
    #array_push_demo()
    #array_push_each_demo()
    #array_push_slice_demo()
    #array_as_set_demo()
    # array_addToSet_demo()
    #array_remove_demo()
    # array_remove_demo2()

    # create_posts()
    #array_position_modifier_demo()

    #array_position_modifier_demo2()
    # array_position_modifier_demo2_2()
    #upsert_demo()
    #or_query()
    #in_query()
    #not_query()
    # null_demo()
    # array_find_demo()
    # array_all_find_demo()
    # array_position_find_demo()
    # array_size_find_demo()
    #runCommand_demo()
    #match_demo()
    # add_demo()
    # cmp_demo()

    SerialNumber = "CBC552555743927"
    DayTime = "2017-11-30"
    # get_sum(SerialNumber, DayTime)

    # insert2()
    # aggregate_demo1()
    # map_reducer_demo()
    # map_reducer_demo2()
    # map_reducer_cond()
    save_map_reduce_result()

if __name__ == '__main__':
    main()