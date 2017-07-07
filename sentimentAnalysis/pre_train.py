#! /usr/bin/env python3

import argparse
import glob
import pickle
import os
import sys

from sklearn.feature_extraction.text import TfidfVectorizer, HashingVectorizer
from sklearn.svm import LinearSVC
from sklearn.linear_model import SGDClassifier


def load(path, sentiment):
    x = []
    for txt in glob.iglob('{}/*.txt'.format(path)):
        with open(txt, 'r') as f:
            x.append(f.read())

    y = [sentiment for _ in range(len(x))]
    return x, y


def load_all(path):
    x_pos, y_pos = load(os.path.join(path, 'pos'), 'pos')
    x_neg, y_neg = load(os.path.join(path, 'neg'), 'neg')

    return x_pos + x_neg, y_pos + y_neg


def build_clf(train):
    x, y = load_all(train)

    vectorizer = HashingVectorizer(stop_words='english')
    vect_x = vectorizer.fit_transform(x)

    clf = SGDClassifier()
    clf.partial_fit(vect_x, y, classes=['pos', 'neg'])

    return [vectorizer, clf]


def save_clf(clf, path):
    with open(path, 'wb+') as f:
        f.write(pickle.dumps(clf))


def logic(argv):
    parser = argparse.ArgumentParser(description='Pre-train of sentiment analysis')
    parser.add_argument('--train', action='store', dest='train', required=True)
    parser.add_argument('--test', action='store', dest='test', required=True)
    parser.add_argument('--clf', action='store', dest='clf', required=True)

    args = parser.parse_args(argv)
    vect, clf = build_clf(args.train)
    save_clf([vect, clf], args.clf)

    x, y = load_all(args.test)
    x_v = vect.transform(x)
    y_v = clf.predict(x_v)

    print((y_v == y).sum() / len(y))


if __name__ == '__main__':
    logic(sys.argv[1:])

