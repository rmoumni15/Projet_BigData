import json
from urllib.error import URLError

from bs4 import BeautifulSoup as soup
from urllib.request import urlopen as uReq
import time

from kafka.admin import NewTopic
from tqdm import tqdm
from movie_propreties import process_movie
from concurrent.futures import ThreadPoolExecutor
import asyncio
from kafka import KafkaProducer, KafkaAdminClient


def get_movies(TOPIC_NAME, utils_predics):
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    admin_client = KafkaAdminClient(
        bootstrap_servers="localhost:9092",
        client_id='test'
    )
    server_topics = admin_client.list_topics()
    """
    if TOPIC_NAME not in server_topics:
        topic = NewTopic(name=str(TOPIC_NAME), num_partitions=1, replication_factor=1)
        admin_client.create_topics(new_topics=topic, validate_only=False)
    """

    async def Scrapper(topic_=TOPIC_NAME, utils_predics= utils_predics) -> list:
        url_top = "https://www.rottentomatoes.com/top/bestofrt/?year=2020"
        url_home = "https://www.rottentomatoes.com"
        audiance_link = "/reviews?type=user"
        try:
            uClient = uReq(url_top).read()

        except URLError:

            return 'FATAL ERROR ACCESSING SERVER.......... RETRY..... '

        top_movies = soup(uClient, "html.parser").find("table", {"class": "table"})
        titles_tag = top_movies.findAll("a", {"class": "unstyled articleLink"})
        ranks = top_movies.findAll("span", {"class": "tMeterScore"})
        Movies = []

        with ThreadPoolExecutor(max_workers=10) as executor:
            loop = asyncio.get_event_loop()
            tasks = [
                loop.run_in_executor(executor, process_movie,
                                     *(topic_, title_tag, rank, url_home, audiance_link, utils_predics))
                for title_tag, rank in tqdm(zip(titles_tag, ranks), total=max([len(titles_tag), len(ranks)]))
            ]
            for response in await asyncio.gather(*tasks):
                if response != 'FAILED':
                    Movies.append(response.__dict__)
        return Movies

    start_time = time.time()
    loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(Scrapper())
    msg = loop.run_until_complete(future)

    # producer.send(str(TOPIC_NAME), json.dumps(msg).encode('utf-8'))

    print(f"====Execution Time : {(time.time() - start_time)} seconds====")

    return str(len(msg)) + " Movies scrapped"
