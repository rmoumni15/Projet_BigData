
import datetime

class Movie:

    def __init__(self, name, rank, genres, score, reviews, langage, director, writer, date_theatre, date_streaming,
                 box_office, duree):
        self.name = str(name)
        self.rank = rank
        self.genres = genres
        self.score = score
        self.reviews = reviews
        self.langage = langage
        self.director = director
        self.writer = writer
        self.date_theatre = date_theatre
        self.date_streaming = date_streaming
        self.box_office = box_office
        self.duree = duree

    def serialize(self):
        res = str(self.name) + "||" + str(self.rank) + "||" + str(self.genres) + "||" + str(self.score) + "||" + str(self.reviews) + "||" \
              + str(self.langage) + "||" + str(self.director) + "||" + str(self.writer) + "||" + str(self.date_theatre) + "||" \
              + str(self.date_streaming) + "||" + str(self.box_office) + "||" + str(self.duree)

        return str(res)




def clean_movie(movie: Movie):

    """
    box_off = movie.box_office
    if box_off != str(np.NaN):
        box_off = float(box_off.split(".")[0].split("$")[0] + "." + box_off.split(".")[1].split("M")[0])
        movie.box_office = box_off
    """
    try:
        if movie.duree != 0:
            time = movie.duree

            if "h" in time:
                hours = int(time.split('h')[0]) * 60
                minutes = int(time.split(' ')[1].split('m')[0])
                movie.duree = hours + minutes
            else:
                minutes = int(time.split('m')[0])
                movie.duree = minutes
    except:
        movie.duree = 0


    try:
        date_th = movie.date_theatre
        date_st = movie.date_streaming

        if date_th != 'unknown':
            date_th = datetime.datetime.strptime(" ".join(date_th.split(",")), "%b %d %Y").strftime("%d/%m/%Y")

        if date_st != 'unknown':
            date_st = datetime.datetime.strptime(" ".join(date_st.split(",")), "%b %d %Y").strftime("%d/%m/%Y")

        movie.date_theatre = date_th
        movie.date_streaming = date_st

        return movie

    except:
        return movie






