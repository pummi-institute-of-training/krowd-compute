import json

INPUT_FILE_PATH_ENTITY = '/Users/sanjeetkumar/Documents/Krowd/Deliveries/London-April2022/tripadvisor_london_april2022_reviews-topic-composition.txt'  
OUTPUT_FILE_PATH_ENTITY = '/Users/sanjeetkumar/Documents/Krowd/Deliveries/London-April2022/tripadvisor_london_april2022_reviews-topic-composition-spark.txt'

topics_entities = []
topics_users = []

def process(line, topics):
    values = line.strip('\n').split('\t')
    filename = values[1].replace("file:/home/ubuntu/TRIPADVISOR_LONDON_APRIL2022/TRIPADVISOR_LONDON_APRIL2022_FORMATTED_BIGRAM/entity_","")
    topic_distribution = list(map(float, values[2:]))
    topics.append(json.dumps({"id": filename.replace(".txt",""), "topicDistributions": topic_distribution}))

with open(INPUT_FILE_PATH_ENTITY, "r") as text_file:
    next(text_file)
    for line in text_file:
        process(line, topics_entities)

with open(OUTPUT_FILE_PATH_ENTITY, "w") as text_file:
    text_file.write("[")

with open(OUTPUT_FILE_PATH_ENTITY, "a") as text_file:
    text_file.writelines("%s,\n" % l for l in topics_entities)


with open(OUTPUT_FILE_PATH_ENTITY, "a") as text_file:
    text_file.write("]")

