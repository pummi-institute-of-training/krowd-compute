### CrossBorder Computation for krowd Score
###### Objective 
* We want to create a single Mysql Table for each category irrespective of cities and plotforms. 
* The compute will happen at city label and the final set of attributes will be pushed to cental document i.e `KrowdRecommendation` in MongoDB which will be single point of connection for cross-border app.(If not mongodb then same table we will use to populate a central table in MYSQL)
* Compute should avoid any kind of manual intervention for computing a new city i.e Full Automation except changing the configuration file.

###### Set of attributes required to be pushed into `KrowdRecommendation`

* entity_url
* name
* address
* cuisines
* logo_url
* website 
* krowd_rating
* krowd_score
* update_at
* other review summary for an entity

###### CrossBorderComputation
* `CrossBorderComputation`

    * requirements.txt
    * entity_summary.py
    * logger.py
    * Readme.md
    * .gitignore
    * config.py

##### Computation Logic 

* Overall procedure 

    * Pull all the data for a given city
    * Get reviews list for each city and create a reviewer-review-data-frame with filtered columns 
    * compute review summary for each entity  
    * compute reviewer rank
    * compute review rank
    * compute krowd score and krowd rating 
    * Compute and add entity basic details 
    * save document to mongodb

##### Steps to run 

* steps
    * pip3 install -r requirements.txt
    * modify the config.py file  
        * To point it to correct mongodb database and document
        * Add credential for mongodb connection
        * Check the global variables like website
    * python3 entity_summary.py


### Author

Krowd R & D India Private Limited

Sanjeet Kumar