#--------------------------Mongodb-------------------------------------------------------------------------------
import urllib.parse
import pymongo
from pymongo import MongoClient

# ------------------ Mongodb Connection ------------------------------------------------------------------------
host = "18.135.51.40"
port = 27709
user_name = "mongoadmin"
pass_word = "krowd_7x6t_mongodb_admin"
db_name = "admin" # database name to authenticate

WEBSITE = 'TripAdvisor'
LATEST_REVIEW_DATE = '07/20/2022'   #'%m/%d/%Y'
MONGODB_CONNECTION_STRING=f'mongodb://{user_name}:{urllib.parse.quote_plus(pass_word)}@{host}:{port}/{db_name}'

#-------------- Mongodb Constants ------------------------------------------------------------------------------
MONGODB_CATEGORY_DB =  "TripadvisorRestaurantsNew"  # "YelpRestaurants" #"YelpBeautyAndSpas" #"YelpHomeServices"

MONGODB_SURROUNDINGS_COLLECTION = 'Surrounding'
MONGODB_CITIES_COLLECTION = "City"
MONGODB_CARD_COLLECTION = "Card"
MONGODB_HOMEPAGE_COLLECTION = "LV"
#MONGODB_ENTITY_COLLECTION =  "Restaurant"
MONGODB_ENTITY_COLLECTION =  "PV"

MONGODB_RECOMMENDATION_DB = 'KrowdRecommendations'
MONGODB_RECOMMENDATION_RESTAURANT = 'RestaurantRecommendationLondonJuly2022'
MONGODB_RECOMMENDATION_HOTEL = 'HotelRecommendation'

def mongodb_connection():
    connection = pymongo.MongoClient(MONGODB_CONNECTION_STRING)
    return connection

connection = mongodb_connection()
