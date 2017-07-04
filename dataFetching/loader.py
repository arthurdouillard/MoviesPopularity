#! /usr/bin/env python3

import argparse
import sys
import json
from kafka import KafkaProducer

def read_file(name):
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    with open(name, 'r') as f:
        json_data = json.load(f)

        for obj in json_data:
            producer.send('movies', json.dumps(json_data[obj]).encode())

    producer.close()

def argparser(argv):
    parser = argparse.ArgumentParser(description="Loader")
    parser.add_argument('--file', action='store', dest='file')
    args = parser.parse_args(argv)
    read_file(args.file)

if __name__ == '__main__':
    argparser(sys.argv[1:])
    