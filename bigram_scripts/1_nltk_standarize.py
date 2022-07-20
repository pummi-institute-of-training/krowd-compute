import os
import nltk
import spacy
from nltk.tokenize import regexp_tokenize, word_tokenize
from nltk.stem import WordNetLemmatizer
from nltk.collocations import BigramAssocMeasures, BigramCollocationFinder
from multiprocessing import Pool, Value
import itertools
from nltk.corpus import PlaintextCorpusReader
import pickle
import re
import time
from config import ROOT,ROOT_FOLDER,REVIEW,FORMATTED,FORMATTED_BIGRAM


nltk.data.path.append('/nltk_data/')

counter = None
#parser = spacy.load('en',max_length=7000000)
parser = spacy.load('en_core_web_sm')
def load_bigram_finder():
    ROOT_DIR = os.path.join(ROOT,ROOT_FOLDER)
    corpus_root = os.path.join(ROOT_DIR,ROOT_FOLDER+FORMATTED)

    bigram_file_path = os.path.join(ROOT_DIR,'replace_dict.p')
    min_freq = 3
    replace_dict = {}
    if os.path.isfile(bigram_file_path):
        replace_dict = pickle.load(open(bigram_file_path, 'rb'))
    else:
        newcorpus = PlaintextCorpusReader(corpus_root, '.*', encoding='utf-8')
        finder = BigramCollocationFinder.from_words(newcorpus.words())
        bgm = BigramAssocMeasures()
        finder.apply_freq_filter(min_freq)
        finder.apply_word_filter(lambda w: len(w) <= 2)
        # scored = finder.score_ngrams(bgm.pmi) # Also supports likelihood_ratio, dunning's g2.
        filtered_list = finder.nbest(bgm.likelihood_ratio, 150000)
        for i in range(0, len(filtered_list)):
            key = filtered_list[i][0].lower() + " " + filtered_list[i][1].lower()
            value = filtered_list[i][0].lower() + "_" + filtered_list[i][1].lower()
            replace_dict[key] = value
        pickle.dump(replace_dict, open(bigram_file_path, 'wb'))
    return replace_dict

def raw_string(s):
    if isinstance(s, str):
        s = s.encode('string-escape')
    elif isinstance(s, unicode):
        s = s.encode('unicode-escape')
    return s

def replace_all(text):
    dict_to_replace = load_bigram_finder()
    reg_dict = re.compile("|".join(map(re.escape, dict_to_replace.keys())))
    return reg_dict.sub(lambda mo: dict_to_replace[mo.group(0)], text)

def readfiles(filenames):
    for entity_name in filenames:
        with open(entity_name, 'r') as myfile:
            yield myfile.read().replace('\n', '')

def standardize(entity_name):
    with open(entity_name, 'r') as myfile:
        data=myfile.read().replace('\n', '')
    tokens = word_tokenize(data)
    pos_tags = nltk.pos_tag(tokens)
    filtered_list = filter(lambda x: x[1] in list_of_tags, pos_tags)
    file_filtered_string = " ".join(map(lambda x: wnl.lemmatize(x[0]), filtered_list)).lower()
    return file_filtered_string.encode('utf-8')

def standardize_spacy(entity_name):
    with open(entity_name, 'r') as myfile:
        data = myfile.read().replace('\n', '').lower()
    tokens = parser(data)
    tags = []
    for token in tokens:
        tags.append((token.lemma_,token.tag_))
    filtered_list = filter(lambda x: x[1] in list_of_tags, tags)
    # file_filtered_string = replace_all(" ".join(map(lambda (x,y): x, filtered_list)), dict_to_replace)
    file_filtered_string = " ".join(map(lambda x: x[0], filtered_list)).lower()
    return file_filtered_string.encode('utf-8')

def getDataFromFile(filename):
    with open(filename, 'r') as myfile:
        data = myfile.read().replace('\n', '').lower()
    return data

def replace_strings(entity_name):
    with open(entity_name, 'r') as myfile:
        data = myfile.read().replace('\n', '').lower()
    data = replace_all(data)
    return data

def writeBigramFile(path_sorted):
    global counter
    with counter.get_lock():
        counter.value += 1
    data = replace_strings(path_sorted).encode('utf-8') + '\n'.encode('utf-8')
    path_sorted_output_filepath = path_sorted.replace(os.path.join(ROOT_DIR,ROOT_FOLDER+FORMATTED),os.path.join(ROOT_DIR,ROOT_FOLDER+FORMATTED_BIGRAM))
    with open(path_sorted_output_filepath, "wb") as text_file:
        text_file.write(data)
        print("file {0},{1} standardized.".format(path_sorted_output_filepath, counter.value))


def init(args):
    ''' store the counter for later use '''
    global counter
    counter = args

def writeStandardizedFile(path_sorted):
    ROOT_DIR = os.path.join(ROOT,ROOT_FOLDER)
    global counter
    with counter.get_lock():
        counter.value += 1
    #after cleaning the original reviews, the cleaned reviews will be saved to folder 'trip_london_formatted'
    path_sorted_output_filepath = path_sorted.replace(os.path.join(ROOT_DIR,ROOT_FOLDER+REVIEW),os.path.join(ROOT_DIR,ROOT_FOLDER+FORMATTED))
    with open(path_sorted_output_filepath, "wb") as text_file:
        text_file.write(standardize_spacy(path_sorted))
        print("file {0},{1} standardized.".format(path_sorted_output_filepath, counter.value))

if __name__ == '__main__':
    start = time.time()
    print("Cleaning of reviews started at ",start)
    list_of_tags = ['CD','FW','JJ','JJR','JJS','NN','NNP','NNPS','NNS','RB','RBR','RBS']
    wnl = WordNetLemmatizer()
    dict_to_replace ={}
    #reg_dict = '';
    
    ROOT_DIR = os.path.join(ROOT,ROOT_FOLDER)
    print(ROOT_DIR)
     

    path = os.path.join(ROOT_DIR,ROOT_FOLDER+REVIEW) #path to the original reviews collection of restaurants is 'trip_london'
    try :
        filepaths_in_directory = [os.path.join(path,fn) for fn in next(os.walk(path))[2]]
        paths_sorted = sorted(filepaths_in_directory, key=os.path.getsize, reverse=False)
        counter = Value('i', 0)
        print("Standardization Started")
        p = Pool(processes = 16, initializer = init, initargs = (counter, ))
        p.map(writeStandardizedFile, paths_sorted)
        print("Cleaning of reviews completed in Min:"+str((time.time() - start)/60.0))
    except StopIteration:
        pass
    
    #print("Standardization Completed, bigram generation started!.")
    #dict_to_replace = load_bigram_finder()
    #reg_dict = re.compile("|".join(map(re.escape, dict_to_replace.keys())))
    #paths_sorted = map(lambda x: x.replace("trip_london","trip_london_formatted"), paths_sorted)
    #p.map(writeBigramFile, paths_sorted)
