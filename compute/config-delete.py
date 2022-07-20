#--------------------------Mongodb-------------------------------------------------------------------------------
import urllib.parse
import pymongo
from pymongo import MongoClient

# ------------------ Mongodb Connection ------------------------------------------------------------------------
host = ""
port = 27017
user_name = ""
pass_word = "" 
db_name = "" # database name to authenticate

MONGODB_CONNECTION_STRING=f'mongodb://{user_name}:{urllib.parse.quote_plus(pass_word)}@{host}:{port}/{db_name}'

#-------------- Mongodb Constants ------------------------------------------------------------------------------
MONGODB_CATEGORY_DB =  "YelpRestaurants"  # "YelpRestaurants" #"YelpBeautyAndSpas" #"YelpHomeServices"

MONGODB_SURROUNDINGS_COLLECTION = 'Surrounding'
MONGODB_CITIES_COLLECTION = "City"
MONGODB_CARD_COLLECTION = "Card"
MONGODB_HOMEPAGE_COLLECTION = "Homepage"
MONGODB_ENTITY_COLLECTION =  "Restaurant" #"Restaurant"  # 'Entity'

MONGODB_RECOMMENDATION_DB = 'KrowdRecommendations'
MONGODB_RECOMMENDATION_RESTAURANT = 'RestaurantRecommendation'
MONGODB_RECOMMENDATION_HOTEL = 'HotelRecommendation'

def mongodb_connection():
    connection = pymongo.MongoClient(MONGODB_CONNECTION_STRING)
    return connection 

connection = mongodb_connection()