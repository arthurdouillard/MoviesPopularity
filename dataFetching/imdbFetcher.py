#! /usr/bin/env python3

import argparse
import json
import sys

from bs4 import BeautifulSoup
from kafka import KafkaProducer
import requests


def fetch_page(url, args):
    while True:
        try:
            response = requests.get(url)
            return response
        except KeyboardInterrupt:
            if args.verbose:
                print('Exiting program after keyboard interrupt.')
            sys.exit(1)
        except:
            if args.verbose:
                print('No internet...\r', end='')


def parse_movies_list(args):
    url = 'http://www.the-numbers.com/movie/budgets/all'
    html = fetch_page(url, args).text
    soup = BeautifulSoup(html.encode('utf-8'), 'html.parser')
    producer = KafkaProducer(bootstrap_servers=args.kafka)
    parse_dollars = lambda x: int(x.replace('$', '').replace(',', ''))

    movies = soup.find_all('td')
    movies_data = []
    spaces = ' ' * 80
    for c, i in enumerate(range(0, 6 * args.max, 6)):
        name = movies[i+2].text.strip()

        if args.verbose:
            print('{}\r'.format(spaces), end='')
            percent = c / args.max * 100
            print('{} %\t{}\r'.format(percent, name), end='')

        movie_data = {}

        movie_data['budget'] = parse_dollars(movies[i+3].text)
        movie_data['gross'] = parse_dollars(movies[i+5].text)
        movie_data.update(get_imdb_data(name, args))
        movies_data.append(movie_data)

        producer.send(args.topic, json.dumps(movie_data).encode())

        if args.save is not None and c % args.step == 0:
            save_data(args.save, movies_data)

    if args.save is not None:
        save_data(args.save, movies_data)


def get_imdb_data(name, args):
    url_imdb = 'http://www.imdb.com'

    # Research with name
    url = '{}/find?ref_=nv_sr_fn&q={}&s=tt'.format(url_imdb, name)
    response = fetch_page(url, args)
    if response.status_code != 200:
        return {}

    # Scrapping movie data
    soup = BeautifulSoup(response.text, 'html.parser')
    try:
        movie_url = soup.find('td', 'result_text').a['href']
    except:
        return {} # No movie found on imdb.
    response = fetch_page('{}{}'.format(url_imdb, movie_url), args)
    if response.status_code != 200:
        return {}
    movie = parse_movie(BeautifulSoup(response.text.encode('utf-8'),
                                      'html.parser'))

    reviews_url = '{}/title/{}/reviews?ref_=tt_urv'\
                    .format(url_imdb, movie_url.split('/')[2])
    response = fetch_page(reviews_url, args)
    if response.status_code != 200:
        return {}
    reviews = parse_reviews(BeautifulSoup(response.text.encode('utf-8'),
                                          'html.parser'))

    movie.update({'reviews': reviews})
    return movie


def parse_reviews(soup):
    reviews = []
    for review in soup.find_all('h2'):
        data = {}
        data['title'] = review.text
        try:
            data['score'] = float(review.next.next.next['alt'].split('/')[0])
        except:
            continue # No mark to a review means the review is discarded.

        data['content'] = review.parent.next_sibling.next_sibling.text
        reviews.append(data)
    return reviews


def parse_movie(soup):
    """
    This is ugly, forgive me.
    """
    movie = {}

    try:
        movie['title'] = soup.title.text[:-7]
    except:
        movie['title'] = ''

    try:
        movie['genres'] = [
                genre.text
                for genre in soup.find_all('span', attrs={'itemprop':'genre'})
     ]
    except:
        movie['genres'] = []

    try:
        movie['score'] = float(soup.find('span', attrs={'itemprop': 'ratingValue'}).text)
    except:
        movie['score'] = .0

    try:
        movie['year'] = int(soup.find('span', id='titleYear').text[1:-1])
    except:
        movie['year'] = 0

    try:
        movie['director'] = soup.find('span', attrs={'itemprop': 'director'}).text.strip()
    except:
        movie['director'] = ''

    try:
        movie['actors'] = [
                actor.text.strip().replace(',', '')
                for actor in soup.find_all('span', attrs={'itemprop': 'actors'})
        ]
    except:
        movie['actors'] = []

    return movie


def save_data(path, data):
    with open(path, 'w+') as f:
        f.write(json.dumps(data, indent=2))


def file2kafka(args):
    producer = KafkaProducer(bootstrap_servers=args.kafka)
    with open(args.load, 'r') as f:
        json_data = json.load(f)

        for obj in json_data:
            producer.send(args.topic, json.dumps(json_data[obj]).encode())


def logic(argv):
    parser = argparse.ArgumentParser(description="Fetcher")
    parser.add_argument('--kafka', action='store', dest='kafka', required=True)
    parser.add_argument('--topic', action='store', dest='topic', required=True)
    parser.add_argument('--max', action='store', dest='max', type=int,
                        default=5000)
    parser.add_argument('--verbose', action='store_true', dest='verbose')
    parser.add_argument('--save', action='store', dest='save')
    parser.add_argument('--load', action='store', dest='load')
    parser.add_argument('--step', action='store', dest='step', type=int,
                        default=100)

    args = parser.parse_args(argv)

    if args.load is not None:
        file2kafka(args)

    parse_movies_list(args)

if __name__ == '__main__':
    logic(sys.argv[1:])
