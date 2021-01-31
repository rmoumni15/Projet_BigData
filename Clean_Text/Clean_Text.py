import re
import unicodedata
import nltk
from string import digits
from bs4 import BeautifulSoup #Nettoyage d'HTML

digits_list = digits


class CleanText:

    def __init__(self):

        french_stopwords = nltk.corpus.stopwords.words('english')
        self.stopwords = [self.remove_accent(sw) for sw in french_stopwords]

        self.stemmer = nltk.stem.SnowballStemmer('english')

    @staticmethod
    def remove_html_code(txt):
        txt = BeautifulSoup(txt, "html.parser", from_encoding='utf-8').get_text()
        return txt

    @staticmethod
    def convert_text_to_lower_case(txt):
        return txt.lower()

    @staticmethod
    def remove_accent(txt):
        return unicodedata.normalize('NFD', txt).encode('ascii', 'ignore').decode("utf-8")

    @staticmethod
    def remove_non_letters(txt):
        return re.sub('[^a-z_]', ' ', txt)

    def remove_stopwords(self, txt):
        return [w for w in txt.split() if (w not in self.stopwords)]

    def get_stem(self, tokens):
        return [self.stemmer.stem(token) for token in tokens]