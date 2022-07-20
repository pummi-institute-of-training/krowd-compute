
#run the krowd score calculation scripts
node ./bin/0_raigor_entity_summary
node ./bin/1_raigor_entity_helpfulness
node ./bin/2_new_raigor_rank_of_reviewer
node ./bin/3_raigor_entity_rank

#run bigram creation scripts

#node ./scripts/bigram_scripts/0_raigor_review_concat_entity
#python3 1_nltk_standarize.py 
#python3 2_nltk_standarize_bigram.py 


# run model generation scripts




