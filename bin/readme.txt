The scripts withing bin folder is node.js script and it will be used in krowd score calculation.
number at the start of every script name represent the order in which the script should be run.

0_raigor_entity_summary 
    =>
1_raigor_entity_helpfulness
    =>

2_raigor_rank_of_reviewer
    =>

3_raigor_entity_rank
    =>


Created a knex.js file for database connection utilising it every where it is used. This will avoid changing database configuration in all scripts.
This will make sure that we will have less security risk and maintiaining it will be simple.

To specify node space. By default it will be low.
 node --max_old_space_size=10240 0_raigor_entity_summary 



++++++++++++++++++++++++after crawl from zomato+++++++++++++++++++++++++++
update user table
remove 'Followers','Follower' from follwer count 
UPDATE users SET follower_count = replace(replace(follower_count,"Followers",""),"Follower","") 

remove 'Review', 'Reviews' from review_count in users table
UPDATE users SET review_count = replace(replace(review_count,"Reviews",""),"Review","") 

in reviews table remove 'Rated' from rating columns
UPDATE reviews SET rating = replace(rating,"Rated","")

UPDATE service_entity SET price = replace(price,"for two people (approx.)","")