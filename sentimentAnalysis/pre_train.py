#! /usr/bin/env python3

import argparse
import glob
import pickle
import sys

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.svm import LinearSVC


def load(path, sentiment):
    x = []
    for txt in glob.iglob('{}/*.txt'.format(path)):
        with open(txt, 'r') as f:
            x.append(f.read())

    y = [sentiment for _ in range(len(x))]
    return x, y


def build_clf(x, y):
    vectorizer = TfidfVectorizer(min_df=5,
                                 max_df = 0.8,
                                 sublinear_tf=True,
                                 use_idf=True)
    vect_x = vectorizer.fit_transform(x)

    clf = LinearSVC()
    clf.fit(vect_x, y)
    return [vectorizer, clf]


def save_clf(clf, path):
    with open(path, 'wb+') as f:
        f.write(pickle.dumps(clf))


def logic(argv):
    parser = argparse.ArgumentParser(description='Pre-train of sentiment analysis')
    parser.add_argument('--pos', action='store', dest='pos', required=True)
    parser.add_argument('--neg', action='store', dest='neg', required=True)
    parser.add_argument('--clf', action='store', dest='clf', required=True)

    args = parser.parse_args(argv)
    x_pos, y_pos = load(args.pos, 'pos')
    x_neg, y_neg = load(args.neg, 'neg')
    clf = build_clf(x_pos + x_neg, y_pos + y_neg)
    save_clf(clf, args.clf)


if __name__ == '__main__':
    logic(sys.argv[1:])

