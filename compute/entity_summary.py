from config import connection, MONGODB_ENTITY_COLLECTION,MONGODB_CATEGORY_DB,MONGODB_RECOMMENDATION_DB,MONGODB_RECOMMENDATION_RESTAURANT
import pandas as pd
import ast
from datetime import datetime
import math
import warnings
import json
from logger import logger
warnings.filterwarnings('ignore')

def fetch_city_dataset(city,country):
    """[Fetch city dataset from MongoDB]

    A['reviewer_id', 'reviewer_photo_count', 'reviewer_review_count', 'reviewer_friend_count', 'review_id', 'review_rating', 'review_date', 'crawled_date', 'review_funny_count', 'review_useful_count', 'review_cool_count', 'review_photo_count']rgs:
        city ([str]): [city name as per recorded in database]

    Returns:
        [df]: [A dataframe with entity details and all reviews contained in list]
    """
    db = connection[MONGODB_CATEGORY_DB]
    entitycollection = db[MONGODB_ENTITY_COLLECTION]
    try:
        total_records = entitycollection.find({"city":city,"country":country}).count()
        logger.info("Total Records for the city - {} is {}".format(city,total_records))
        batch_size = 500
        current_index = 0 
        all_dfs = []
        logger.info("Started fetching data for city : {}".format(city))
        while current_index < total_records:
            logger.info("Started Fetching records from index : {}".format(current_index))
            results = entitycollection.find({"city": city,'country':country}, {"entity_url": 1, "name": 1,"cuisines":1, "city": 1,
                                                        "address": 1, "logo_url": 1,'reviews': 1, "_id": 0}).skip(current_index).limit(batch_size)
            results = list(results)
            logger.info("Fetching of city data completed started creating dataframe.")
            sliced_df = pd.DataFrame(results)
            all_dfs.append(sliced_df)
            current_index = current_index + batch_size
            
        df = pd.concat(all_dfs)
        logger.info("Dataframe got created successfully and it is of shape : {} \n with head \n {}".format(
            df.shape, df.head()))
        return df
    except Exception as e:
        logger.info("Error occurred while fetch city data for city : {} \n {}".format(city, e))


def process_rating(rating):
    """[summary]

    Args:
        rating ([type]): [description]

    Returns:
        [type]: [description]
    """
    return rating

def calculate_entity_summary(entity_url, ratings):
    """[Generate Entity Level Review Rating summary]
    Args:
        entity_url ([str]): [entity url representing actual part like /biz/lago-de-guija-los-angeles ]
        rating ([list of rating]): [contains all the reviews rating of the entity as list ]

    Returns:
        [dictionary]: [containing attributes - entity_url,one_start_count,...,five_star_count,+ve and -ve review count]
    """
    rating_count = len(ratings)
    one_star_count = len(
        [item for item in ratings if item == 1 or item == 1.5 or item == 1.0])
    two_star_count = len(
        [item for item in ratings if item == 2 or item == 2.5 or item == 2.0])
    three_star_count = len(
        [item for item in ratings if item == 3 or item == 3.5 or item == 3.0])
    four_star_count = len(
        [item for item in ratings if item == 4 or item == 4.5 or item == 4.0])
    five_star_count = len([item for item in ratings if item == 5 or item == 5.0])

    positive_reviews_count = three_star_count + four_star_count + five_star_count
    negative_reviews_count = one_star_count + two_star_count

    if rating_count > 0:
        score = one_star_count + (2 * two_star_count) + (3 * three_star_count) + \
            (4 * four_star_count) + (5 * five_star_count)
        final_rating = round((score / (rating_count)),4)
    else:
        final_rating = 0

    result = {"entity_url": entity_url, "one_star_count": one_star_count, "two_star_count": two_star_count, "three_star_count": three_star_count, "four_star_count": four_star_count,
              "five_star_count": five_star_count, "rating_count": rating_count, "positive_reviews_count": positive_reviews_count, "negative_reviews_count": negative_reviews_count, "final_rating": final_rating}

    return result


def calculate_reviewer_helpfullness(reviewer_rank_value,avg,std):
    """[Calculates Rank of a Reviewer]

    Args:
        reviewer_rank_value ([int]): [reviewer attribute value which we want to use for calculating reviewer rank]
        avg ([float]): [average value of reviewer_rank_key column]
        std ([float]): [standard deviation value of reviewer_rank_key column]

    Returns:
        [float]: [helpfullness score of the reviewer]
    """
    helpfullness_score = 1
    if (reviewer_rank_value-avg) > 0:
        helpfullness_score = 1 + ((reviewer_rank_value - avg) / std)
        
    return helpfullness_score


def calculate_review_helpfullness(review_rank_value,avg,std):
    """[Calculates Rank of a Review]

    Args:
        review_rank_value ([int]): [ review attribute value which we want to use for calculating review rank eg helpfull_count ] 
        avg ([float]): [average value of review_rank_key column]
        std ([float]): [standard deviation value of review_rank_key column]

    Returns:
        [type]: [helpfullness score of the review]
    """
    helpfullness_score = 1 
    if ( review_rank_value - avg) > 0:
        helpfullness_score = 1 + ((review_rank_value - avg) / std)
        
    return helpfullness_score



def calculate_reviewer_rank(unique_reviewer_df):
    
    """[Creating a dictionary of reviewer_rank]

    Args:
        unique_reviewer_df ([dataframe]): [dataframe consisting of unique reviewer]

    Returns:
        [dictionary]: [ a dictionary containing reviewer_id as key and reviewer_rank as value]
    """
    reviewer_rank_key = 'reviewer_friend_count'
    std = unique_reviewer_df[reviewer_rank_key].describe()['std']
    avg = unique_reviewer_df[reviewer_rank_key].describe()['mean']
    logger.info("Reviewer rank key is {} \n Average : {} \t Std : {}".format(reviewer_rank_key,avg,std))
    for index,row in unique_reviewer_df.iterrows():
        unique_reviewer_df.loc[index,'reviewer_rank'] = calculate_reviewer_helpfullness( row[reviewer_rank_key],avg,std)
    reviewer_rank_dict = dict(zip(unique_reviewer_df['reviewer_id'], unique_reviewer_df['reviewer_rank'] ))
    
    return reviewer_rank_dict 



def calculate_review_rank(df):
    """[Creating and populating additional column for review rank]

    Args:
        df ([dataframe]): [dataframe containing all the reviews]

    Returns:
        [dataframe]: [ input dataframe with additional column of review rank]
    """
    review_rank_key = 'review_useful_count'
    avg = df[review_rank_key].describe()["mean"]
    std = df[review_rank_key].describe()["std"]
    logger.info(" Review rank key is {} \n Average : {} \t Std : {} ".format(review_rank_key,avg,std))
    for index, row in df.iterrows():
        df.loc[index,"review_rank"] = calculate_review_helpfullness(row[review_rank_key],avg,std)
    
    return df 
    
    
def create_reviewer_review_dateset(df):
    """[Creating reviewer-review dataframe]

    Args:
        df ([dataframe]): [original dataframe from datastore which contains reviews in list]

    Returns:
        [dataframe]: [a dataframe containing reviews in separate row with additional entity level attributes ]
    """
    reviewer_review_dfs = []
    summary_documents = {}
    
    try:
        logger.info("Started Creating User-Review dataframe.")
        for index,row in df.iterrows():

            reviews = row['reviews']
            
            reviews = bytes(str(reviews), encoding='utf-8')
            reviews = reviews.decode('utf-8')
            reviews = ast.literal_eval(reviews)
                        
            entity_url = row['entity_url']

            ratings = [process_rating(review['review_rating']) for review in reviews]
            summary = calculate_entity_summary(entity_url,ratings)
            
            summary_documents[entity_url] = summary 
            temp = pd.DataFrame(reviews)
            temp = temp[['reviewer_id', 'reviewer_photo_count', 'reviewer_review_count', 'reviewer_friend_count', 'review_id', 'review_rating', 'review_date', 'crawled_date', 'review_funny_count', 'review_useful_count', 'review_cool_count', 'review_photo_count']]
            temp['entity_url'] = entity_url
            temp['rating_count'] = summary['rating_count']
            temp['positive_reviews_count'] = summary['positive_reviews_count']
            temp["final_rating"] = summary["final_rating"]
            reviewer_review_dfs.append(temp)
        reviewer_review_combined = pd.concat(reviewer_review_dfs)
        logger.info("Completed Creating Reviewer-Review dataframe.")
        return reviewer_review_combined,summary_documents
    except Exception as e:
        logger.info("Exception occurred while creating reviewer-review dataset : {}".format(e))


def calculate_krowd_score(df):
    """[create two dictionaries one for storing krowd score and another for storing krowd rating]

    Args:
        df ([dataframe]): [dataframe containing columns required to calculate krowd score and krowd rating]

    Returns:
        [dictionary]: [ two dictionaries 1. krowd_score 2. krowd_rating]
    """
    # calculate durability factor for each review
    for index, row in df.iterrows():
        df.loc[index,"durability"] = calculate_durability_factor(row['review_date'],row['crawled_date'])
    
    
    df['product_of_dhs'] = df[['review_rank','reviewer_rank','durability']].apply(lambda x : x['review_rank']*x['reviewer_rank']*x['durability'],axis=1)
    df['product_of_dhrs'] = df[['review_rank','reviewer_rank','durability','review_rating']].apply(lambda x : x['review_rank']*x['reviewer_rank']*x['durability']*x['review_rating'],axis=1)
    
    # calculate wilson score for each entity which is function of rating count and positive reviews count
    wilson_df = df.drop_duplicates('entity_url').reset_index(drop=True)
    for index,row in wilson_df.iterrows():
        wilson_df.loc[index,'wilson_score'] = wilson_score(row['positive_reviews_count'],row['rating_count'])
    wilson_score_dict = dict(zip(wilson_df['entity_url'],wilson_df['wilson_score']))
    
    krowd_score_df = pd.DataFrame(df.groupby('entity_url')['product_of_dhs','product_of_dhrs'].agg({'product_of_dhs':'sum','product_of_dhrs':'sum'}).reset_index())
    krowd_score_df.rename(columns={'product_of_dhs':'sum_of_product_dhs','product_of_dhrs':'sum_of_product_dhrs'},inplace=True)
    
    krowd_score_df['krowd_score'] = krowd_score_df['sum_of_product_dhrs'] / krowd_score_df['sum_of_product_dhs']
    for index,row in krowd_score_df.iterrows():
        krowd_score_df.loc[index,'krowd_rating'] = row['krowd_score']* wilson_score_dict[row['entity_url']]

    krowd_score_dict  = dict(zip(krowd_score_df['entity_url'],krowd_score_df['krowd_score']))
    krowd_rating_dict = dict(zip(krowd_score_df['entity_url'],krowd_score_df['krowd_rating']))
    return krowd_score_dict,krowd_rating_dict 


def wilson_score(pos,n):
    """[calculate wilson score for the given entity]

    Args:
        pos ([int]): [Number Positive reviews for that entity]
        n ([int]): [Total number of ratings for that entity]
['reviewer_id', 'reviewer_photo_count', 'reviewer_review_count', 'reviewer_friend_count', 'review_id', 'review_rating', 'review_date', 'crawled_date', 'review_funny_count', 'review_useful_count', 'review_cool_count', 'review_photo_count']
    Returns:
        [float]: [wilson score]
    """
    if n == 0:
        return 0 
    #Corresponds to an 99.975% confidence on the entities with least amount of reviews.
    z = 3.6622599308876151
    phat = 1 * pos / n
    return (phat + z*z/(2*n) - z * math.sqrt((phat*(1-phat)+z*z/(4*n))/n))/(1+z*z/n);


def process_review_date(review_date,crawled_date):
    """[Cleaning review date]

    Args:
        review_date ([string]): [date on which the review was written]
        crawled_date ([string]): [date on which that record was crawled]

    Returns:
        [string]: [cleaned review date or derived actual date based on crawled_date]
    """
    return review_date


def calculate_durability_factor(review_date,crawled_date):
    """[Calculate the durability of each review]

    Args:
        review_date ([string]): [review date]
        crawled_date ([string]): [crawled date]

    Returns:
        [float]: [durability of review based on review date and ref date]
    """
    review_date = process_review_date(review_date,crawled_date)
    review_date = datetime.strptime(review_date,'%m/%d/%Y')
    ref_date = datetime.strptime("2/1/2021",'%m/%d/%Y')
    diff = (ref_date - review_date).days     # difference should be in days
    return 1 - math.exp(-math.exp(-0.045 * (math.ceil(diff / 7) - 65)))


def save_entity_summary(summary_documents):
    """[Save the final entity level information in MongoDB]

    Args:
        summary_documents ([list of dictionaries]): [list of dictionaries containing entity level information which will be utilized in front end]
    """
    # save documents 
    db = connection[MONGODB_RECOMMENDATION_DB]
    rest_rec_collection = db[MONGODB_RECOMMENDATION_RESTAURANT]
    records = [value for key,value in summary_documents.items()]

    for key,value in summary_documents.items():
        rest_rec_collection.update({"entity_url":key},dict(value),upsert=True)
    logger.info("Completed inserting all records ")

def get_entity_basic_details(df):
    """[Get the basic details about all the entity like name, logo_url, cuisines, address, city]

    Args:
        df ([dataframe]): [original dataset pulled from MongoDB]

    Returns:
        [dictionary]: [a dictionary containing entity_url as key and basic details as values]
    """
    basic_column_list = ['entity_url','name','cuisines','address','logo_url','city']

    entity_basic_df = df.drop_duplicates('entity_url').reset_index(drop=True)
    entity_basic_df = entity_basic_df[basic_column_list]
    entity_basic_dict = {}
    for index,row in entity_basic_df.iterrows():
        temp = {}
        temp['name'] = row['name']
        temp['address'] = row['address']
        temp['city'] = row['city']
        temp['cuisines'] = row['cuisines']
        temp['logo_url'] = row['logo_url']
        temp['website'] = 'Yelp'
        entity_basic_dict[row['entity_url']] = temp 
        
    return entity_basic_dict

def compute_driver(city):
    """[This is the driver function which facilitate all the compute ]

    Args:
        city ([string]): [input city name]
    """
    df = fetch_city_dataset(city)                                                          
    # df.to_csv("city-dataset.csv",header=True,index=False)
    # df = pd.read_csv("city-dataset.csv")

    df = df[df['reviews'].notna()] # dropping entities which don't have reviews so it will not so up in the input suggestion also
    df.reset_index(drop=True,inplace=True)
    
    entity_basic_dict = get_entity_basic_details(df) # get entity basic details like name, address, logo url, cuisines, city


    # create reviewer review dataframe with additional columns like rating_count,positive_reviews_count,entity_url,final_rating
    reviewer_review_combined,summary_documents = create_reviewer_review_dateset(df) 
    
    # add review rank in the reviewer_review_combined dataframe
    reviewer_review_combined = calculate_review_rank(reviewer_review_combined)
    
    # calculate each reviewer rank 
    unique_reviewer_df = reviewer_review_combined.drop_duplicates('reviewer_id')
    reviewer_rank_dict = calculate_reviewer_rank(unique_reviewer_df)
    reviewer_review_combined['reviewer_rank'] = reviewer_review_combined['reviewer_id'].apply(lambda x : reviewer_rank_dict[x])
    
    final_columns = ['review_rank','reviewer_rank','review_date','crawled_date','final_rating','rating_count','positive_reviews_count','entity_url','review_rating']
    
    # calculate krowd score and krowd rating
    krowd_score_dict,krowd_rating_dict = calculate_krowd_score(reviewer_review_combined[final_columns])
    

    
    for key,value in summary_documents.items():
        value['krowd_score'] = krowd_score_dict[key]
        value['krowd_rating'] = krowd_rating_dict[key]
        value['name'] = entity_basic_dict[key]['name']
        value['address'] = entity_basic_dict[key]['address']
        value['city'] = entity_basic_dict[key]['city']
        value['cuisines'] = entity_basic_dict[key]['cuisines']
        value['logo_url'] = entity_basic_dict[key]['logo_url']
        value['updated_at'] = str(datetime.now())
    save_entity_summary(summary_documents)
    
    logger.info("saving completed")




def test_script():
    ## ------------------------------- calculate_entity_summary test starts ---------------------------------------------
    # Expected input
    entity_url = 'http://www.test.com'
    ratings = [1,2,3,4,5,1,2,3,4,5]
    # Expected output
    summary_expected_result = {'entity_url': 'http://www.test.com', 'one_star_count': 2, 'two_star_count': 2, 'three_star_count': 2, 'four_star_count': 2, 'five_star_count': 2, 'rating_count': 10, 'positive_reviews_count': 6, 'negative_reviews_count': 4, 'final_rating': 3.0}
    summary = calculate_entity_summary(entity_url,ratings)
    
    if summary == summary_expected_result:
        logger.info("Test Passed")
    else:
        logger.info("Entity summary creation function is not correct - calculate_entity_summary(entity_url,ratings)")

    ## ------------------------------- calculate_entity_summary test ends------------------------------------------------
    ## ----- calculating helpfullness of reviewer test starts here---------
    reviewer_key_avg = 149.0
    reviewer_key_std = 335.0
    reviewer_key_val = 160.0
    reviewer_rank_expected = 1.0328358208955224
    reviewer_rank = calculate_reviewer_helpfullness(reviewer_key_val,reviewer_key_avg,reviewer_key_std)
    
    if reviewer_rank == reviewer_rank_expected:
        logger.info("Reviewer Rank calculation Test Passed")
    else:
        logger.info("Reviewer Rank calculation Test Failed")
    ## ----- calculating helpfullness of reviewer test ends here---------
        
    ## ----- calculating helpfullness of review test starts here---------
    review_key_avg = 1.27
    review_key_std = 3.86
    review_key_val = 2 
    review_rank_expected = 1.189119170984456
    review_rank = calculate_review_helpfullness(review_key_val,review_key_avg,review_key_std)
    
    if review_rank == review_rank_expected:
        logger.info("Review Rank calculation Test Passed")
    else:
        logger.info("Review Rank calculation Test Failed")
    ## ----- calculating helpfullness of review test ends here---------
    
    ## ----- calculating krowd score-krowd rating of entity starts here---------
    krowd_score_test_df = pd.DataFrame({"entity_url":['http:x.com','http:x.com','http:x.com'],
                                        "rating_count":[3,3,3],
                                        "review_rating":[5,4,2],
                                        "review_date":["03/04/2020","06/12/2020","07/10/2019"],
                                        "crawled_date":["12/03/2021","12/03/2021","12/03/2021"],
                                        "reviewer_rank":[1,1,1],
                                        "review_rank":[1,1,1],
                                        "positive_reviews_count":[2,2,2]})
    krowd_score_dict,krowd_rating_dict = calculate_krowd_score(krowd_score_test_df)
    logger.info("Krowd Score Dict : {}".format(krowd_score_dict))
    logger.info("Krowd Rating Dict : {} ".format(krowd_rating_dict))
        
if __name__=='__main__':
    city = "Los Angeles"
    compute_driver(city)
    # test_script()