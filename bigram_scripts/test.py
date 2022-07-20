from config import ROOT,ROOT_FOLDER,REVIEW,FORMATTED,FORMATTED_BIGRAM,TOPIC_MODEL,MODEL
import os


ROOT_DIR = os.path.join(ROOT,ROOT_FOLDER)
review_path = os.path.join(ROOT_DIR,ROOT_FOLDER+REVIEW)

formatted = os.path.join(ROOT_DIR,ROOT_FOLDER+FORMATTED)
formatted_bigram = os.path.join(ROOT_DIR,ROOT_FOLDER+FORMATTED_BIGRAM)

model = os.path.join(ROOT_DIR,ROOT_FOLDER+MODEL)
topicmodel = os.path.join(ROOT_DIR,ROOT_FOLDER+TOPIC_MODEL)

print(review_path)
print(formatted)
print(formatted_bigram)
print(ROOT_DIR)
print(model)
print(topicmodel)