# Krowd-Restaurant

This project is for performing bi-gram creation, topic modelling and model generation (which is required by restaurant app recommendation)


1. update the directory location in config.py
2. update the the db credential in knex.js fiel

## presteps
1. Make folders which are there in config.py

## steps
1. Review Collection for each restaurant. (scripts/bigram_scripts/0_raigor_review_concat_entity)
2. Cleaning of reviews per document. (scripts/bigram_scripts/1_nltk_standarize.py)
3. Bi-gram creation (scripts/bigram_scripts/2_nltk_standarize_bigram.py)
4. Topic Modeling (scripts/topic_modelling_scripts/topicmodelling.sh)

## Install on linux
1. sudo apt-get update
2. sudo apt-get upgrade
3. curl -sL https://deb.nodesource.com/setup_14.x | sudo -E bash -
4. sudo apt-get install -y nodejs
5. sudo apt install npm
6. npm install fs
7. npm install console-stamp
8. sudo apt install openjdk-8-jre-headless 
9. wget http://mallet.cs.umass.edu/dist/mallet-2.0.8.zip  (Install mallet on root folder(In our case it is /mnt))
10. unzip mallet-2.0.8.zip 
11. vi .bashrc (put this at botton of file 'export PATH=$PATH:/usr/java/jre1.8.0_292/bin/' )
12. python3 -m spacy download en_core_web_sm











model json
"id":"619097f7a0a59d74fd385b65","ThaKrowdScore":2.8387352926,"CuisineList":["Asian","Thai","Malaysian"],"latlong":"51.495026,-0.18354","name":"Umami","priceRange":2,"rank":189,"reviewCount":1089,"topicDistributions":






Todo
 1. convert 0_raigor_review_concat_entity into 
 2. 619269a0a0a59d74fd515cdc got missed in topic modelling even when reviews were available
 3. include model json file creation logic within compute logic itself(compute folder)


 