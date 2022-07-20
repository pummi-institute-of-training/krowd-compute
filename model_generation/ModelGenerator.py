import json

#--------------------------Mongodb-------------------------------------------------------------------------------
import urllib.parse
import pymongo
from pymongo import MongoClient
from bson.objectid import ObjectId
import json 
from datetime import datetime
# ------------------ Mongodb Connection ------------------------------------------------------------------------
host = "34.121.16.224"
port = 27017
user_name = "mongoadmin"
pass_word = "krowd_7x6t_mongodb_admin"  
db_name = "admin"  # database name to authenticate
MONGODB_CONNECTION_STRING=f'mongodb://{user_name}:{urllib.parse.quote_plus(pass_word)}@{host}:{port}/{db_name}'
#-------------- Mongodb Constants ------------------------------------------------------------------------------
MONGODB_RESTAURANT_DB = "TripadvisorRestaurantsNew"
MONGODB_COUNTRIES_COLLECTION = "Country"
MONGODB_CITIES_COLLECTION = "City"
MONGODB_CARD_COLLECTION = "Card"
MONGODB_HOMEPAGE_COLLECTION = "Homepage"
MONGODB_RESTAURANT_COLLECTION = "Restaurant"

MONGODB_RECOMMENDATION_DB = 'KrowdRecommendations'
MONGODB_RECOMMENDATION_RESTAURANT = 'RestaurantRecommendation'


def mongodb_connection_one():
    connection = pymongo.MongoClient(MONGODB_CONNECTION_STRING)
    db = connection[MONGODB_RESTAURANT_DB]
    return db 

def mongodb_connection_two():
    connection = pymongo.MongoClient(MONGODB_CONNECTION_STRING)
    db = connection[MONGODB_RECOMMENDATION_DB]
    return db 


input_file_location = '/home/ubuntu/mnt/TRIPADVISOR_JAKARTA/TRIPADVISOR_JAKARTA_TOPICMODEL/tripadvisor_jakarta_reviews-topic-composition_spark.json'
output_file_location = '/home/ubuntu/mnt/TRIPADVISOR_JAKARTA/TRIPADVISOR_JAKARTA_MODEL/model.json'


def save_to_file(json_array):
    with open(output_file_location, 'w') as json_file:
        json.dump(json_array, json_file)
    

def generate_data():
    input_file = open (input_file_location)
    json_array = json.load(input_file)
    db_1 = mongodb_connection_one()
    db_2 = mongodb_connection_two()
    output_array = []
    count = 0

    for item in json_array:
        id = item["id"]
        temp = db_1[MONGODB_RESTAURANT_COLLECTION].find_one({"_id":ObjectId(id)}, {"entity_url":1, "name":1, "latlong":1,"cuisines":1, "_id":0})
        entity_url = temp['entity_url']
        CuisineList = temp['cuisines']
        latlong = temp['latlong']
        name = temp['name']
        krowd_score = db_2[MONGODB_RECOMMENDATION_RESTAURANT].find_one({"entity_url":entity_url}, {"krowd_score":1, "_id": 0})
        
        data = {
            "id": 'service_'+str(id),
            "name": name,
            "topicDistributions": item["topicDistribution"],
            "ThaKrowdScore": krowd_score['krowd_score'],
            "priceRange": 16.00,
            "latlong": latlong
        }
        output_array.append(data)
        count = count + 1
        print(count)
        
    save_to_file(output_array)
    print("Done")    


if __name__=='__main__':
    generate_data()




