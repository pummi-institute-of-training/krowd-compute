import ast
import json
import os

def standardize_json(folder_path,file_name):

    with open(os.path.join(folder_path,file_name),'r') as f:
        data = f.read()
        data = data.replace("\n",',')
        data = list(ast.literal_eval(data))
        
        with open(os.path.join(folder_path,'model.json'),'w') as r:
            json.dump(data,r)

if __name__=='__main__':

    folder_path = '/mnt/ZOMATO_SYDNEY_JAN2020/ZOMATO_SYDNEY_JAN2020_MODEL'
    file_name = 'part-r-00000-a3614347-6512-416a-a1e3-9a60a5b56037.json'
    standardize_json(folder_path,file_name)

    print("Standardization of json file completed")
