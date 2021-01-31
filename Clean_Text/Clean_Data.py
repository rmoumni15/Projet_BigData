import datetime

from Spark.movie import Movie
import numpy as np


def clean_movie(movie: Movie):
    """
    box_off = movie.box_office
    if box_off != str(np.NaN):
        box_off = float(box_off.split(".")[0].split("$")[0] + "." + box_off.split(".")[1].split("M")[0])
        movie.box_office = box_off

    if movie.duree != str(np.NaN):
        time = movie.duree

        if "h" in time:
            hours = int(time.split('h')[0]) * 60
            minutes = int(time.split(' ')[1].split('m')[0])
            movie.duree = hours + minutes
        else:
            minutes = int(time.split('m')[0])
            movie.duree = minutes
    """
    date_th = movie.date_theatre
    date_st = movie.date_streaming

    if date_th != 'unknown':
        date_th = datetime.datetime.strptime(" ".join(date_th.split(",")), "%b %d %Y").strftime("%d/%m/%Y")

    if date_st != 'unknown':
        date_st = datetime.datetime.strptime(" ".join(date_st.split(",")), "%b %d %Y").strftime("%d/%m/%Y")

    movie.date_theatre = date_th
    movie.date_streaming = date_st

    return movie
