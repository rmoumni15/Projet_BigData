from asyncio import sleep

from Api import get_movies_api
from Scrapper import get_movies
from tensorflow import keras
import pickle
from elasticsearch import Elasticsearch
import tweepy.error as te

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
    limitation = False
    while True:
        if limitation is False:
            try:
                print("------------------------API---------------------")
                movies_api = get_movies_api('movies', utils_predics)

                print(movies_api)
            except te.RateLimitError:
                print('API Limit Exceeded Switching to Scrapper only.......')
                limitation = True
        print("------------------------Scrapper---------------------")
        movies_scrapper = get_movies('movies', utils_predics)

        print(movies_scrapper)

        sleep(5)
