
from Scrapper import get_movies
import nltk
from tensorflow import keras
import pickle
from elasticsearch import Elasticsearch


if __name__ == '__main__':
    es = Elasticsearch()
    utils_predics = {
        'model': keras.models.load_model('Model _Tokenizer/Model_GRU'),
        'tokenizer': pickle.load(open("Model _Tokenizer/tokenizer.pickle", "rb")),
        'max_row': pickle.load(open("Model _Tokenizer/max_row.pickle", "rb"))
    }
    print("Model Loaded.....")

    es.indices.create(index='movies-index', ignore=400)

    print('Index configured.....')


    while True:
        test = get_movies('movies', utils_predics)
        print(test)
