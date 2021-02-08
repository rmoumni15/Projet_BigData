from keras_preprocessing.sequence import pad_sequences
import numpy as np


def predict_score(movie, utils_predics):
    if len(movie.reviews) > 0:
        seq_reviews = pad_sequences(utils_predics['tokenizer'].texts_to_sequences(movie.reviews), utils_predics['max_row']
                                    , padding='post')
        seq_reviews = utils_predics['model'].predict(seq_reviews)
        pred = [(np.argmax(x) + 1) / 5 for x in seq_reviews]
        movie.reviews = np.round(np.mean(pred), 2)

    else:
        movie.reviews = 0
    return movie