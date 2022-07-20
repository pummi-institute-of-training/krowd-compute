#! /bin/bash

#change this path according to your folder

BIGRAM_PATH='/home/ubuntu/TRIPADVISOR_LONDON_APRIL2022/TRIPADVISOR_LONDON_APRIL2022_FORMATTED_BIGRAM/'
TOPICMODELLING_OUTPUTPATH='/home/ubuntu/TRIPADVISOR_LONDON_APRIL2022/TRIPADVISOR_LONDON_APRIL2022_FORMATTED_BIGRAM/'

cd /home/ubuntu/mnt


cd mallet-2.0.8

#convert the input-bigram-txt to mallet internal format
bin/mallet import-dir --input $BIGRAM_PATH  --output $TOPICMODELLING_OUTPUTPATH/bigrams_input.mallet --keep-sequence --remove-stopwords   

#trainig the topic model
bin/mallet train-topics --input $TOPICMODELLING_OUTPUTPATH/bigrams_input.mallet  --num-topics 180  --num-iterations 2000  --num-threads 8  --optimize-interval 20  --optimize-burn-in 200  --num-top-words 30  --output-model-interval 500  --output-model $TOPICMODELLING_OUTPUTPATH/tripadvisor_london_april2022.model  --word-topic-counts-file $TOPICMODELLING_OUTPUTPATH/tripadvisor_london_april2022_words_topic_counts.txt  --topic-word-weights-file $TOPICMODELLING_OUTPUTPATH/tripadvisor_london_april2022_topic_word_weights.txt --output-state $TOPICMODELLING_OUTPUTPATH/tripadvisor_london_april2022_lda_restaurents.state.gz  --output-state-interval 500  --output-doc-topics $TOPICMODELLING_OUTPUTPATH/tripadvisor_london_april2022_reviews-topic-composition.txt  --output-topic-keys $TOPICMODELLING_OUTPUTPATH/tripadvisor_london_april2022_quick-look.txt  --xml-topic-report $TOPICMODELLING_OUTPUTPATH/tripadvisor_london_april2022_xml_topic_report.xml

