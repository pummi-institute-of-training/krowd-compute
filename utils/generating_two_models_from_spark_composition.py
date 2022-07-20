import mysql.connector
import pandas as pd
import ast
import json
import csv
from google.cloud import secretmanager


global_spark_file = '/media/krowd/Data/CrossBorderSampled/GlobalTopicModelling-180-sampled/reviews-topic-composition_sampled_spark.txt'
global_london_spark_file = '/media/krowd/Data/CrossBorderSampled/GlobalTopicModelling-180-sampled/global_london180-reviews-topic-composition_sampled_spark.txt'
global_dubai_spark_file = '/media/krowd/Data/CrossBorderSampled/GlobalTopicModelling-180-sampled/global-dubai180-reviews-topic-composition_sampled_spark.txt'

PROJECT_ID = 'alpha-274108'

def access_secret_version(self,project_id, secret_id, version_id):
    """
    Access the payload for the given secret version if one exists. The version
    can be a version number as a string (e.g. "5") or an alias (e.g. "latest").
    """
    # Create the Secret Manager client.
    client = secretmanager.SecretManagerServiceClient()

    # Build the resource name of the secret version.
    name = client.secret_version_path(project_id, secret_id, version_id)

    # Access the secret version.
    response = client.access_secret_version(name)
    
    payload = response.payload.data.decode('UTF-8')
    return format(payload)


HOSTNAME = access_secret_version(PROJECT_ID, 'CHOST-A', 'latest'),
USER = access_secret_version(self.PROJECT_ID, 'CHOST-A-USER', 'latest'),
PASSWORD = access_secret_version(self.PROJECT_ID, 'CHOST-A-PASS', 'latest'),
DATABASE = 'sydney_zomato_database'

def get_connection():
    mydb = mysql.connector.connect(
                host=HOSTNAME,
                user=USER,
                passwd=PASSWORD,
                database=DATABASE
    )

    mycursor = mydb.cursor()

    return mycursor,mydb 



def generateFile(df):

    mycursor,mydb = get_connection()
    #for london
    outputfile_name = global_london_spark_file

    #for dubai
    #connection parameter ("","","","")
    #outputfile_name = global_dubai_spark_file
    print("database query started!!")
    mycursor.execute("SELECT entity_id FROM entity_summary")
    myresult = mycursor.fetchall()
    print("all result fetched from database!!")
    myresult = [item[0] for item in myresult]
    df = df[df.id.isin(myresult)]




    with open(outputfile_name,'w') as f:
        for row in df.iterrows():
            row[1].to_json(f)
            f.write("\n")
    print(outputfile_name +" saved")


if __name__=='__main__':
    print("In main function")
    with open(global_spark_file) as f:
        data = ast.literal_eval(f.read())

    df = pd.DataFrame(data)

    print("\nGlobal Spark \n",df.head())

    generateFile(df)


