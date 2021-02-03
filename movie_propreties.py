from urllib.error import HTTPError

from kafka import KafkaProducer
from keras_preprocessing.sequence import pad_sequences
from tqdm import tqdm
from urllib.request import urlopen as uReq
from bs4 import BeautifulSoup as soup
import numpy as np
from Clean_Text.Clean_Text import CleanText
from Spark.movie import Movie

cleaner = CleanText()


def apply_all_transformation(txt):
    cleaned_txt = cleaner.remove_html_code(txt)
    cleaned_txt = cleaner.convert_text_to_lower_case(cleaned_txt)
    cleaned_txt = cleaner.remove_accent(cleaned_txt)
    cleaned_txt = cleaner.remove_non_letters(cleaned_txt)
    cleaned_txt = cleaner.remove_stopwords(cleaned_txt)
    cleaned_txt = cleaner.get_stem(cleaned_txt)
    return cleaned_txt


def clean_df_column(column):
    return [" ".join(apply_all_transformation(x)) for x in tqdm(column, desc="CleanReviews")]


def get_movie_propreties(movie_parser):
    movie_propreties = movie_parser.find("ul", {"class": "content-meta info"})
    test = movie_propreties.findAll("li")
    prop = {}
    for elem in tqdm(test, total=len(test)):
        tmp = elem.findAll("div")
        label = tmp[0].next.strip()
        if label == "Director:":
            value = tmp[1].find("a").next
            prop[str(label.split(':')[0])] = " ".join(value.split())
        elif label == "Producer:":
            value = tmp[1].findAll("a")
            values = {" ".join(elem.next.split()) for elem in value}
            prop[str(label.split(':')[0])] = values
        elif label == "Writer:":
            value = tmp[1].find("a").next
            prop[str(label.split(':')[0])] = " ".join(value.split())
        elif label == "Release Date (Theaters):":
            value = tmp[1].find("time").next
            prop[str(label.split(':')[0])] = " ".join(value.split())
        elif label == 'Release Date (Streaming):':
            value = tmp[1].find("time").next
            prop[str(label.split(':')[0])] = " ".join(value.split())
        elif label == "Runtime:":
            value = tmp[1].find("time").next
            prop[str(label.split(':')[0])] = " ".join(value.split())
        else:
            prop[label.split(':')[0]] = " ".join(tmp[1].next.split())

    return prop


def process_movie(topic, title_tag, rank, url_home, audiance_link, utils_predics):
    producer = KafkaProducer(bootstrap_servers='localhost:9092')

    title = str(title_tag.next).strip()
    movie_link = title_tag.get("href")
    try:
        movie_page = uReq(url_home + movie_link).read()
    except HTTPError as e:
        print(f"ERROR can't access to server LINK : {url_home + movie_link}  SKIPING.........")
        return 'FAILED'

    movie_data = soup(movie_page, "lxml")
    movie_propreties = get_movie_propreties(movie_data)


    score = movie_data.find("score-board", {"class": "scoreboard"})["audiencescore"]
    if score:
        score = int(score) * 1e-2
    else:
        score = 0

    genres = movie_propreties['Genre'] if 'Genre' in movie_propreties else 'unknown'
    original_language = movie_propreties['Original Language'] if 'Original Language' in movie_propreties else 'unknown'
    director = movie_propreties['Director'] if 'Director' in movie_propreties else 'unknown'
    writer = movie_propreties['Writer'] if 'Writer' in movie_propreties else 'unknown'
    date_theatre = movie_propreties[
        'Release Date (Theaters)'] if 'Release Date (Theaters)' in movie_propreties else 'unknown'
    date_streaming = movie_propreties[
        'Release Date (Streaming)'] if 'Release Date (Streaming)' in movie_propreties else 'unknown'
    box_office = movie_propreties[
        'Box Office (Gross USA)'] if 'Box Office (Gross USA)' in movie_propreties else 0
    duree = movie_propreties['Runtime'] if 'Runtime' in movie_propreties else 0
    reviews = [elem.p.next.strip() for elem in movie_data.find("div", {"id": "reviews"}).find_all("li")]

    try:
        movie_AD = uReq(url_home + movie_link + audiance_link).read()
    except HTTPError as e:
        print(f"ERROR can't access to server LINK : {url_home + movie_link + audiance_link}  SKIPING.........")
        return 'FAILED'

    reviews_AD = soup(movie_AD, "lxml")
    reviews_audiance = reviews_AD.find("div", {"id": "movieUserReviewsContent"})
    res = []
    if reviews_audiance:
        reviews_audiance = reviews_AD.find("div", {"id": "movieUserReviewsContent"}).findAll("li", {
            "class": "audience-reviews__item"})
        res = [reviews_audiance[i].find("p", {
            "class": "audience-reviews__review js-review-text clamp clamp-8 js-clamp"}).next for i in
               range(len(reviews_audiance))]
    reviews = reviews + res
    reviews = list(clean_df_column(reviews))
    movie = Movie(name=title, rank=int(rank.next.split('%')[0]) * 1e-2, genres=genres, score=score, reviews=reviews,
                  langage=original_language,
                  director=director, writer=writer, date_theatre=date_theatre, date_streaming=date_streaming,
                  box_office=box_office, duree=duree)

    ##### BETA #######

    if len(movie.reviews) > 0:
        seq_reviews = pad_sequences(utils_predics['tokenizer'].texts_to_sequences(movie.reviews), utils_predics['max_row']
                                    , padding='post')
        seq_reviews = utils_predics['model'].predict(seq_reviews)
        pred = [(np.argmax(x) + 1) / 5 for x in seq_reviews]

        movie.reviews = np.round(np.mean(pred), 2)
    else:
        movie.reviews = 0

    #### BETA ########

    producer.send(str(topic), bytes(movie.serialize(), encoding='utf-8'))
    return movie
