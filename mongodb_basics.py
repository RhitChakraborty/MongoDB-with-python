# -*- coding: utf-8 -*-
"""
Created on Wed Oct  7 21:10:43 2020

@author: rhitc
"""

import pymongo
client=pymongo.MongoClient('mongodb://127.0.0.1/')

mydb=client['Employee']
information=mydb.employeeinformation
records=[{
    'firstname':'Rhit',
    'last_name':'Chak',
    'department':'analytics',
    'qualification':'phd',
    'age':26
    },
    {
    'firstname':'Ribhu',
    'last_name':'Chak',
    'department':'full Stack',
    'qualification':'M.Sc',
    'age':31
    },
    {
    'firstname':'oin',
    'last_name':'Chak',
    'department':'SQL',
    'qualification':'B.sc',
    'age':31
    }
    ]

information.insert_many(records) #insert_one for one data

#getting the first record
information.find_one() 

#getting all recoreds

for record in information.find():
    print(record)


## Querrying based on equality conditions
for rec in information.find({'firstname':'Ribhu'}):
    print(rec)
    
    
"""Querry documents using Querry operation($in,$lt,$gt,$or,$and)"""
for rec in information.find({'qualification':{'$in':['M.Sc','phd']}}):
    print(rec)



## Using And and Querry
for rec in information.find({'qualification':{'$in':['M.Sc','phd']},'age':{'$lt':30}}):
    print(rec)
    
    
# using OR operator 
for rec in information.find({'$or':[{'firstname':'Rhit'},{'qualification':'M.Sc'}]}):
    print(rec)
    
##Creating new Collection Inventory (collection ==Tables in SQL)
inventory=mydb.inventory 
inventory.insert_many( [
   { 'item': "journal", 'qty': 25, 'size': { 'h': 14, 'w': 21,'uom': "cm" }, 'status': "A" },
   { 'item': "notebook", 'qty': 50,'size': { 'h': 8.5, 'w': 11,'uom': "in" },'status': "A" },
   { 'item': "paper", 'qty': 100, 'size': { 'h': 8.5, 'w': 11,'uom': "in" },'status': "D" },
   { 'item': "planner", 'qty': 75, 'size': { 'h': 22.85,'w': 30,'uom': "cm" },'status': "D" },
   { 'item': "postcard", 'qty': 45, 'size': { 'h': 10, 'w': 15.25,'uom': "cm" },'status': "A" },
   {"item": "sketchbook","qty": 80,"size": {"h": 14, "w": 21, "uom": "cm"},"status": "A"},
   {"item": "sketch pad","qty": 95,"size": {"h": 22.85, "w": 30.5, "uom": "cm"},"status": "A"}
])   

#nested Json Documents
for rec in inventory.find({'size':{ 'h': 14, 'w': 21,'uom': "cm" }}):
    print(rec)
    
########################################################################    
""" Update Json Documents-- update_one(),update_many(),replace_one()"""
#Update one
inventory.update_one({'item':'sketch pad'},{'$set':{'size.uom':'mm','status':'P'},
                                            "$currentDate":{"lastModified":True}})

#update many
inventory.update_many({'qty':{'$lt':50}},
                      {'$set':{'size.uom':'in','status':'P'},
                       "$currentDate":{'LastModified':True}})

#Replace a particular record with new record
inventory.replace_one(
    {"item": "paper"},
    {"item": "paper",
     "instock": [
         {"warehouse": "A", "qty": 60},
         {"warehouse": "B", "qty": 40}]})

#######################################################################
""" Aggregate function-- Agv,sum, project"""
studentscore=mydb['studentscores']  
data = [ 
    {"user":"Rhit", "subject":"Database", "score":80}, 
    {"user":"Amit",  "subject":"JavaScript", "score":90}, 
    {"user":"Amit",  "title":"Database", "score":85}, 
    {"user":"Rhit",  "title":"JavaScript", "score":75}, 
    {"user":"Amit",  "title":"Data Science", "score":60},
    {"user":"Rhit",  "title":"Data Science", "score":95}] 
  
studentscore.insert_many(data)

### Find Amit And Rhit Total Subjects
agg_result=studentscore.aggregate([{"$group":{"_id":'$user',
                                  'Total subjects':{'$sum':1}}}])
for i in agg_result:
    print(i)

### Total Score based on user
total_Scores=studentscore.aggregate([{'$group':{'_id':'$user',
                                      "Total Score":{'$sum':'$score'}}}])
for i in total_Scores:
    print(i)
    
    
### Average score based on user
avg_scores=studentscore.aggregate([{'$group':{'_id':'$user',
                                      "Average Score":{'$avg':'$score'}}}])
for i in avg_scores:
    print(i)
    
    
    
    
###############################################3
import datetime

### Create a new collection
data=[{ "_id" : 1, "item" : "abc", "price" : 10, "quantity" : 2, "date" : datetime.datetime.utcnow()},
{ "_id" : 2, "item" : "jkl", "price" : 20, "quantity" : 1, "date" : datetime.datetime.utcnow() },
{ "_id" : 3, "item" : "xyz", "price" : 5, "quantity" : 5, "date" : datetime.datetime.utcnow() },
{ "_id" : 4, "item" : "abc", "price" : 10, "quantity" : 10, "date" : datetime.datetime.utcnow() },
{ "_id" : 5, "item" : "xyz", "price" : 5, "quantity" : 10, "date" :datetime.datetime.utcnow() }]

store=mydb['Store']
store.insert_many(data)



##Calculating the average quantity And Average Price

agg_result=store.aggregate([
    {'$group':{"_id":'$item',
               'average amount':{"$avg":{'$multiply':['$price','$quantity']}},
               'average quantity':{'$avg':'$quantity'}}}
    
    ])

for i in agg_result:
    print(i)



""" $Project""" #select columns from table

data=[{
  "_id" : 1,
  "title": "abc123",
  "isbn": "0001122223334",
  "author": { "last": "zzz", "first": "aaa" },
  "copies": 5
},
{
  "_id" : 2,
  "title": "Baked Goods",
  "isbn": "9999999999999",
  "author": { "last": "xyz", "first": "abc", "middle": "" },
  "copies": 2
}
]

books=mydb['Books']
books.insert_many(data)

for i in books.aggregate([
    {'$project':{'title':1,'isbn':1}}]):
    print(i)















