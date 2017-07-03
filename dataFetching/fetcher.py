#! /usr/bin/env python3

import argparse
import json
import sys
import time

import requests


def make_request(query, api):
    while True:
        try:
            query = 'https://api.themoviedb.org/3/movie/{}?api_key={}'\
                    .format(query, api)
            response = requests.get(query)
            return response
        except KeyboardInterrupt:
            print('Exiting program after keyboard interrupt.')
            sys.exit(1)
        except:
            print('No internet...\r', end='')


def get_max_id(api):
    response = make_request('latest', api)
    if response.status_code != 200:
        raise Exception(response.json())

    return response.json()['id']


def get_movie_info(movie_id, api):
    response = make_request(str(movie_id), api)
    if response.status_code != 200:
        return None, None

    movie = response.json()
    return movie['original_title'], movie['vote_average']


def fetch_reviews(movie_id, api):
    response = make_request('{}/reviews'.format(movie_id), api)
    if response.status_code != 200:
        return None

    reviews = response.json()
    reviews_content = []
    for review in reviews['results']:
        reviews_content.append(review['content'])

    return reviews_content


def fetch_all_data(min_id, max_id, api, verbose, step):
    data = {}
    req_count = 0
    for c, i in enumerate(range(min_id, max_id+1)):
        if step is not None and c % step == 0:
            save_data('tmp_{}.json'.format(c), data)
            if verbose:
                print(' ' * 30 + '\r', end='')
                print('Saving tmp...\r', end='')

        if verbose:
            percent = (i - min_id) / (max_id - min_id) * 100
            print(' ' * 30 + '\r', end='')
            print('Progress:\t{} %\r'.format(round(percent, 2)), end='')

        movie_name, rating = get_movie_info(i, api)
        req_count += 1
        if movie_name is None:
            continue
        reviews = fetch_reviews(i, api)
        req_count += 1
        if reviews is None or len(reviews) == 0:
            continue
        data[i] = {
                'name': movie_name,
                'rating': rating,
                'reviews': reviews
        }

        if req_count >= 38:
            req_count = 0
            time.sleep(10)

    return data


def save_data(name, data):
    with open(name, 'w+') as f:
        f.write(json.dumps(data, indent=2))


def argparser(argv):
    parser = argparse.ArgumentParser(description="Fetcher")
    parser.add_argument('--api', action='store', dest='api')
    parser.add_argument('--min', action='store', dest='min_id', type=int)
    parser.add_argument('--max', action='store', dest='max_id', type=int)
    parser.add_argument('--verbose', action='store_true', dest='verbose')
    parser.add_argument('--save', action='store', dest='save', required=True)
    parser.add_argument('--step', action='store', dest='step', type=int)

    args = parser.parse_args(argv)
    if args.api is None:
        args.api = '77579a46faf6f45da3142e4cb4303e01'
    if args.max_id is None:
        args.max_id = get_max_id(args.api)
    if args.min_id is None:
        args.min_id = 1

    if args.verbose:
        print('Fetching movies from {} to {} (included).'\
              .format(args.min_id, args.max_id))
    data = fetch_all_data(args.min_id, args.max_id, args.api, args.verbose,
                          args.step)
    save_data(args.save, data)


if __name__ == '__main__':
    argparser(sys.argv[1:])
