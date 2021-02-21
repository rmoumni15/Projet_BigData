import asyncio
import time
from concurrent.futures.thread import ThreadPoolExecutor

from imdb import IMDb
import tweepy
from kafka import KafkaProducer


from Spark.movie import Movie
from predict import predict_score
from movie_propreties import clean_df_column

ia = IMDb()
CONSUMMER_KEY = "###########"
CONSUMMER_SECRET = "##############################"
auth = tweepy.OAuthHandler(CONSUMMER_KEY, CONSUMMER_SECRET)

api = tweepy.API(auth)


def get_movies_api(topic, utils_predics):
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
        movie = predict_score(movie, utils_predics)
        producer.send(str(topic), bytes(movie.serialize(), encoding='utf-8'))
        return movie

    async def movies_apî(Topic=topic , utils_predics= utils_predics):

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
    print(f"====Execution Time : {(time.time() - start_time)} seconds====")

    return str(len(msg)) + " Movies from API"
