#! /usr/bin/env python3

import argparse
import json
import pickle
import sys

from kafka import KafkaConsumer, KafkaProducer


def get_movie(args):
    consumer = KafkaConsumer(args.src, auto_offset_reset='earliest')
    for msg in consumer:
        yield json.loads(msg.value)


def load_clf(path):
    with open(path, 'rb') as f:
        return pickle.loads(f.read())


def incorporate_sentiments(movie, sentiments):
    sent_chooser = lambda sent: 0 if sent == 'neg' else 1
    for i in range(len(movie['reviews'])):
        movie['reviews'][i].update({'sentiment': sent_chooser(sentiments[i])})

    return movie


def classify_sentiments(args):
    producer = KafkaProducer(bootstrap_servers=args.kafka)
    vectorizer, clf = load_clf(args.clf)
    spaces = ' ' * 80

    for movie in get_movie(args):
        if args.verbose:
            print('{}\r'.format(spaces), end='')
            print('{}\r'.format(movie['title'].strip()), end='')

        reviews = extract_reviews(movie)
        if len(reviews) == 0:
            continue

        # Prediction
        vect_reviews = vectorizer.fit_transform(reviews)
        sentiments = clf.predict(vect_reviews)

        movie = incorporate_sentiments(movie, sentiments)

        producer.send(args.dst, json.dumps(movie).encode())

        clf.partial_fit(vect_reviews, extract_scores(movie),
                        classes=['pos', 'neg'])


def extract_scores(movie):
    scorer = lambda x: 'pos' if x > 5 else 'neg'
    return [scorer(review['score']) for review in movie['reviews']]


def extract_reviews(movie):
    return [review['content'] for review in movie['reviews']]


def logic(argv):
    parser = argparse.ArgumentParser(description='Sentiment Analyser')
    parser.add_argument('--src', action='store', dest='src',
                        required=True)
    parser.add_argument('--dst', action='store', dest='dst',
                        required=True)
    parser.add_argument('--kafka', action='store', dest='kafka',
                        required=True)
    parser.add_argument('--clf', action='store', dest='clf',
                        required=True)
    parser.add_argument('--verbose', action='store_true', dest='verbose')

    args = parser.parse_args(argv)
    classify_sentiments(args)


if __name__ == '__main__':
    logic(sys.argv[1:])

