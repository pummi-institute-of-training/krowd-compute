#collecting the reviews from mongodb
node 0_raigor_review_concat_entity

#for cleaning the reviews
python3 1_nltk_standarize.py 

#bigram creation
python3 2_nltk_standarize_bigram.py 
