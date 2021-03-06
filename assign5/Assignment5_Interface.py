#!/usr/bin/python2.7
#
# Assignment3 Interface
# Name: Lei Chen 1206139983
#

from pymongo import MongoClient
import os
import sys
import json
import re
import codecs
import math

''' TO Grader:

set CASE_SENSITIVE to
    Ture for better performance
    False for case insensitivity

'''
CASE_SENSITIVE = False


DATABASE_NAME = "ddsassignment5"
COLLECTION_NAME = "businessCollection"
CITY_TO_SEARCH = "tempe"
MAX_DISTANCE = 10
CATEGORIES_TO_SEARCH = ["Fashion", "Food", "Cafes"]
MY_LOCATION = ['33.423426', '-111.939375'] #[LATITUDE, LONGITUDE]
SAVE_LOCATION_1 = "findBusinessBasedOnCity.txt"
SAVE_LOCATION_2 = "findBusinessBasedOnLocation.txt"
SEP='$'
RADIUS_EARTH= 3959


def FindBusinessBasedOnCity(cityToSearch, saveLocation1, collection):
    output = codecs.open(saveLocation1, 'w', encoding='utf-8')
    cursor = collection.find({'city': cityToSearch if CASE_SENSITIVE else re.compile(cityToSearch, re.IGNORECASE)})
    for doc in cursor:
        line= doc['name']+SEP+doc['full_address']+SEP+doc['city']+SEP+doc['state']
        # put each doc in a single line
        line = line.replace('\n', ',') + '\n'
        output.write(line if CASE_SENSITIVE else line.upper())

def FindBusinessBasedOnLocation(categoriesToSearch, myLocation, maxDistance, saveLocation2, collection):
    output = codecs.open(saveLocation2, 'w', encoding='utf-8')
    cursor = collection.find()
    tarCats = set(categoriesToSearch if CASE_SENSITIVE else [cat.upper() for cat in categoriesToSearch])
    for doc in cursor:
        cats = set(doc['categories'] if CASE_SENSITIVE else [cat.upper() for cat in doc['categories']])
        if not tarCats & cats: continue
        if distance_on_unit_sphere(float(myLocation[0]), float(myLocation[1]), doc['latitude'], doc['longitude']) > maxDistance: continue
        output.write((doc['name'] if CASE_SENSITIVE else doc['name'].upper())+'\n')


# code from http://www.johndcook.com/blog/python_longitude_latitude/
def distance_on_unit_sphere(lat1, long1, lat2, long2):
    degrees_to_radians = math.pi/180.0

    # phi = 90 - latitude
    phi1 = (90.0 - lat1)*degrees_to_radians
    phi2 = (90.0 - lat2)*degrees_to_radians

    # theta = longitude
    theta1 = long1*degrees_to_radians
    theta2 = long2*degrees_to_radians

    cos = (math.sin(phi1)*math.sin(phi2)*math.cos(theta1 - theta2) +
           math.cos(phi1)*math.cos(phi2))
    arc = math.acos( cos )

    return arc * RADIUS_EARTH

if __name__ == '__main__':
    try:
        #Getting Connection from MongoDB
        conn = MongoClient('mongodb://localhost:27017/')

        #Creating a New DB in MongoDB
        print "Creating database in MongoDB named as " + DATABASE_NAME
        database   = conn[DATABASE_NAME]

        #Creating a collection named businessCollection in MongoDB
        print "Creating a collection in " + DATABASE_NAME + " named as " + COLLECTION_NAME
        collection = database[COLLECTION_NAME]

        #Finding All Business name and address(full_address, city and state) present in CITY_TO_SEARCH
        print "Executing FindBusinessBasedOnCity function"
        FindBusinessBasedOnCity(CITY_TO_SEARCH, SAVE_LOCATION_1, collection)

        #Finding All Business name and address(full_address, city and state) present in radius of MY_LOCATION for CATEGORIES_TO_SEARCH
        print "Executing FindBusinessBasedOnLocation function"
        FindBusinessBasedOnLocation(CATEGORIES_TO_SEARCH, MY_LOCATION, MAX_DISTANCE, SAVE_LOCATION_2, collection)

    except Exception as detail:
        print "Something bad has happened!!! This is the error ==> ", detail
