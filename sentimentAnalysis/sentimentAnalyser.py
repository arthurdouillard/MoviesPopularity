#! /usr/bin/env python3

import argparse
import json
import pickle
import sys

from kafka import KafkaProducer


def get_movies():
    input_data = sys.stdin.read()
    blop_json = json.loads(input_data)

    if type(blop_json) == list:
        return blop_json
    else:
        return [blop_json]


def load_clf(path):
    with open(path, 'rb') as f:
        return pickle.loads(f.read())


def incorporate_sentiments(movie, sentiments):
    sent_chooser = lambda sent: 0 if sent == 'neg' else 1
    for i in range(len(movie['reviews'])):
        movie['reviews'][i].update({'sentiment': sent_chooser(sentiments[i])})

    return movie


def classify_sentiments(args):
    producer = KafkaProducer(bootstrap_servers=args.brokers)
    vectorizer, clf = load_clf(args.clf)

    for movie in get_movies():
        reviews = extract_reviews(movie)
        if len(reviews) == 0:
            continue

        vect_reviews = vectorizer.transform(reviews)
        sentiments = clf.predict(vect_reviews)

        movie = incorporate_sentiments(movie, sentiments)
        producer.send(args.topic, json.dumps(movie).encode())


def extract_reviews(movie):
    return [review['content'] for review in movie['reviews']]


def logic(argv):
    parser = argparse.ArgumentParser(description='Sentiment Analyser')
    parser.add_argument('--topic', action='store', dest='topic',
                        required=True)
    parser.add_argument('--brokers', action='store', dest='brokers',
                        required=True)
    parser.add_argument('--clf', action='store', dest='clf',
                        required=True)

    args = parser.parse_args(argv)
    classify_sentiments(args)


if __name__ == '__main__':
    logic(sys.argv[1:])

