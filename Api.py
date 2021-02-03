import asyncio
import time
from concurrent.futures.thread import ThreadPoolExecutor

from imdb import IMDb
import tweepy
from kafka import KafkaProducer
from keras_preprocessing.sequence import pad_sequences
from tensorflow import keras
import pickle
import numpy as np

from Spark.movie import Movie
from movie_propreties import clean_df_column

ia = IMDb()
auth = tweepy.OAuthHandler('SCxux1ZWeeT6MiTZQUfoZ6KSw', 'cg4BjefvlUHHKyVn77EsNrmk8xVLqlGoaMErtLmxFC1AOViYXf')

api = tweepy.API(auth)


def get_movies_api(topic, util_predics):
    def api_prop(m, topic, utils_predics):
        producer = KafkaProducer(bootstrap_servers='localhost:9092')
        b = ia.get_movie(str(m.getID()))

        title = b['title']
        rank = (float(m['rating']) / 10) if m.__contains__('rating') else 0
        genres = b['genres'][0] if b.__contains__('genres') else 'unknown'
        score = float(m['rating']) / 10 if m.__contains__('rating') else 0
        langage = b['languages'] if b.__contains__('languages') else 'unknown'
        director = b['directors'][0]['name'] if b.__contains__('directors') else 'unknown'
        writer = b['writer'][0]['name'] if b.__contains__('writer') else 'unknown'
        date_theatre = b['original air date'] if b.__contains__('original air date') else 'unknown'
        date_streaming = b['original air date'] if b.__contains__('original air date') else 'unknown'

        try:
            box_office = str(b['box office']['Budget'].split(',')[0]).split('$')[1]
        except:
            box_office = 'unknown'
        try:
            duree = b['runtimes'][0] if b['runtimes'][0] else 0
        except:
            duree = 0

        reviews = []

        tweet = api.search('#' + title.strip().lower().replace(" ", ""), tweet_mode="extended", count=50,
                           exclude_replies=True)
        for t in tweet:
            reviews.append(t.full_text)

        reviews = list(clean_df_column(reviews))

        movie = Movie(name=title, rank=rank, genres=genres, score=score,
                      reviews=reviews, langage=langage, director=director, writer=writer, date_theatre=date_theatre,
                      date_streaming=date_streaming, box_office=box_office, duree=duree)
        ##### BETA #######

        if len(movie.reviews) > 0:
            seq_reviews = pad_sequences(utils_predics['tokenizer'].texts_to_sequences(movie.reviews),
                                        utils_predics['max_row']
                                        , padding='post')
            seq_reviews = utils_predics['model'].predict(seq_reviews)
            pred = [(np.argmax(x) + 1) / 5 for x in seq_reviews]

            movie.reviews = np.round(np.mean(pred), 2)
        else:
            movie.reviews = 0

        #### BETA ########

        producer.send(str(topic), bytes(movie.serialize(), encoding='utf-8'))
        return movie

    async def movies_apî(Topic=topic, utils_predics=util_predics):

        top_100 = ia.get_popular100_movies()
        res = []

        with ThreadPoolExecutor(max_workers=10) as executor:
            loop = asyncio.get_event_loop()
            tasks = [
                loop.run_in_executor(executor, api_prop,
                                     *(m, Topic, utils_predics))
                for m in top_100
            ]
            for response in await asyncio.gather(*tasks):
                if response != 'FAILED':
                    res.append(response.__dict__)
        return res

    start_time = time.time()
    loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(movies_apî())
    msg = loop.run_until_complete(future)

    # producer.send(str(TOPIC_NAME), json.dumps(msg).encode('utf-8'))

    print(f"====Execution Time : {(time.time() - start_time)} seconds====")

    return str(len(msg)) + " Movies from API"
