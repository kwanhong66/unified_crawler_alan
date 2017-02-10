#!/usr/bin/env bash

PYTHONPATH=$PYTHONPATH:./twitter_stream python3 ./twitter_stream/twitter_streamer.py -o="/home/rnd1/data/twitter_crawler/twitter_stream/" -cmd="keywords" -cc="ko_stopwords"