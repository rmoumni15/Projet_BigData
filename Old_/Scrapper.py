import json

from bs4 import BeautifulSoup as soup
from urllib.request import urlopen as uReq
from Spark.movie import *
import time
from tqdm import tqdm
from movie_propreties import get_movie_propreties

""""
url_top = "https://www.rottentomatoes.com/top/bestofrt/?year=2020"
url_home = "https://www.rottentomatoes.com"

uClient = uReq(url_top).read()
top_movies = soup(uClient, "html.parser")
m = top_movies.find("table", {"class": "table"})
titles = m.findAll("a", {"class": "unstyled articleLink"})
rank = m.findAll("span", {"class": "tMeterScore"})
title = str(titles[0].next).strip()
link = titles[0].get("href")
#print(rank)

#################### Reviews #############################"

movie_page = uReq(url_home + link).read()
reviews_movie = soup(movie_page, "lxml")
review = reviews_movie.find("div", {"id": "reviews"}).find_all("li")[0].p.next.strip()
genres = " ".join(reviews_movie.find("div", {"class": "meta-value genre"}).next.split())
release_date = reviews_movie.findAll("div", {"class": "meta-value"})[6].find("time").next
box_office = reviews_movie.findAll("div", {"class": "meta-value"})[8].next


# print(box_office)

#################### Reviews ############################

audiance_link = "/reviews?type=user"

movie_page_1 = uReq(url_home + link + audiance_link).read()
reviews_AD = soup(movie_page_1, "lxml")
reviews_audiance = reviews_AD.find("div", {"id": "movieUserReviewsContent"}).findAll("li", {"class": "audience-reviews__item"})
res = [reviews_audiance[i].find("p",{"class": "audience-reviews__review js-review-text clamp clamp-8 js-clamp" }).next for i in range(len(reviews_audiance))]
print(len(res))


"""


#### function ####

def Scrapper():
    start_time = time.time()
    url_top = "https://www.rottentomatoes.com/top/bestofrt/?year=2020"
    url_home = "https://www.rottentomatoes.com"
    audiance_link = "/reviews?type=user"

    uClient = uReq(url_top).read()
    top_movies = soup(uClient, "html.parser").find("table", {"class": "table"})
    titles_tag = top_movies.findAll("a", {"class": "unstyled articleLink"})
    ranks = top_movies.findAll("span", {"class": "tMeterScore"})
    Movies = []
    for title_tag, rank in tqdm(zip(titles_tag, ranks), total=max([len(titles_tag), len(ranks)])):
        title = str(title_tag.next).strip()
        movie_link = title_tag.get("href")
        movie_page = uReq(url_home + movie_link).read()
        movie_data = soup(movie_page, "lxml")
        movie_propreties = get_movie_propreties(movie_data)
        # print(title)
        score = movie_data.find("div", {"class": "mop-ratings-wrap__half audience-score"}).findAll("span")
        if score:
            # print(score)
            score = " ".join(score[1].next.split())
        else:
            score = "None"
        genres = movie_propreties['Genre'] if 'Genre' in movie_propreties else 'None'
        original_language = movie_propreties['Original Language'] if 'Original Language' in movie_propreties else 'None'
        director = movie_propreties['Director'] if 'Director' in movie_propreties else 'None'
        writer = movie_propreties['Writer'] if 'Writer' in movie_propreties else 'None'
        date_theatre = movie_propreties[
            'Release Date (Theaters)'] if 'Release Date (Theaters)' in movie_propreties else 'None'
        date_streaming = movie_propreties[
            'Release Date (Streaming)'] if 'Release Date (Streaming)' in movie_propreties else 'None'
        box_office = movie_propreties[
            'Box Office (Gross USA)'] if 'Box Office (Gross USA)' in movie_propreties else 'None'
        duree = movie_propreties['Runtime'] if 'Runtime' in movie_propreties else 'None'

        reviews = [elem.p.next.strip() for elem in movie_data.find("div", {"id": "reviews"}).find_all("li")]

        movie_AD = uReq(url_home + movie_link + audiance_link).read()
        # print(url_home + movie_link + audiance_link)
        reviews_AD = soup(movie_AD, "lxml")
        reviews_audiance = reviews_AD.find("div", {"id": "movieUserReviewsContent"})
        res = []
        if reviews_audiance:
            reviews_audiance = reviews_AD.find("div", {"id": "movieUserReviewsContent"}).findAll("li", {
                "class": "audience-reviews__item"})
            res = [reviews_audiance[i].find("p", {
                "class": "audience-reviews__review js-review-text clamp clamp-8 js-clamp"}).next for i in
                   range(len(reviews_audiance))]
        reviews.append(res)

        Movies.append(
            Movie(name=title, rank=rank.next, genres=genres, score=score, reviews=reviews, langage=original_language,
                  director=director, writer=writer, date_theatre=date_theatre, date_streaming=date_streaming,
                  box_office=box_office, duree=duree))
    print(f"====Execution Time : {(time.time() - start_time)} seconds====")
    return Movies


mymovies = Scrapper()
json_string = json.dumps([ob.__dict__ for ob in mymovies])

with open("movies.json", "w") as f:
    f.write(json_string)

print(len(mymovies))

