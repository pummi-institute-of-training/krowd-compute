import pandas as pd
import mysql.connector
import json
import os
from google.cloud import secretmanager


def getConnection(host_url,user_name,db_passwd,db_name):
    mydb = mysql.connector.connect(
                host=host_url,
                user=user_name,
                passwd=db_passwd,
                database=db_name
    )

    mycursor = mydb.cursor()
    
    sql = 'SELECT t.id,ee.id as group_id FROM (SELECT e.id , r.id as rid, r.group_id FROM entity e, restaurant_group r WHERE r.id = e.entity_id) t, entity ee WHERE ee.entity_id = t.group_id'
    
    mycursor.execute(sql)
    df = pd.DataFrame(mycursor.fetchall())
    df.columns = ['id','group_id']
    return df

def generate_listGroupDict(df,folder_path):
    listGroupdf = pd.DataFrame(df.groupby('group_id')['id'].apply(list))
    listGroupDict = dict(zip(listGroupdf.index,listGroupdf.id))
    listGroupDict = [listGroupDict]
    file_name = 'listGroupDict.json'
    with open(os.path.join(folder_path,file_name), 'w') as fp:
        json.dump(listGroupDict, fp)
    print("listGroupDict is saved to path :",os.path.join(folder_path,file_name))    

def generate_groupDict(df,folder_path):
    groupDict = dict(zip(group_df.id,group_df.group_id))
    groupDict = [groupDict]
    file_name = 'groupDict.json'
    with open(os.path.join(folder_path,file_name), 'w') as fp:
        json.dump(groupDict, fp)

    print("listGroupDict is saved to path :",os.path.join(folder_path,file_name))    

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

if __name__=='__main__':

    group_df = getConnection(HOSTNAME,USER,PASSWORD,DATABASE)

    folder_path = '/mnt/ZOMATO_SYDNEY_JAN2020/ZOMATO_SYDNEY_JAN2020_MODEL'

    generate_groupDict(group_df,folder_path)

    generate_listGroupDict(group_df,folder_path)