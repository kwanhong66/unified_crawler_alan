# -*- coding: utf8 -*-
# !/usr/bin/python3
'''
Copyright 2013 Mark Dredze. All rights reserved.
This software is released under the 2-clause BSD license.
Mark Dredze, mdredze@cs.jhu.edu
'''
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from time import gmtime, strftime, localtime
import gzip, logging, datetime
import time
import math

import json

import os
import sys
from twitter_stream.utils import parseCommandLine, usage

class FileListener(StreamListener):
    def __init__(self, path, restart_time):
        self.path = path
        self.siesta = 0
        self.nightnight = 0
        self.current_file = None
        self.restart_time = restart_time
        self.file_start_time = time.time()
        self.file_start_date = datetime.datetime.now()
        self.kor_tweet_count = 0
        self.non_kor_tweet_count = 0

    def on_data(self, data):
        current_time = datetime.datetime.now()

        if self.current_file == None or time.time() - self.restart_time > self.file_start_time \
        		or self.file_start_date.day != current_time.day:
            self.startFile()
            self.file_start_date = datetime.datetime.now()

        if data.startswith('{'):

            if 'delete' in data:
                # delete = json.loads(data)['delete']['status']
                # if self.on_delete(delete['id'], delete['user_id']) is False:
                # 	return False
                print("delete")
                time.sleep(10)
                return False

            if 'lang' in data:
                jsoned_data = json.loads(data)
                if jsoned_data['lang'] == 'ko':

                    jsoned_data = json.loads(data)
                    # with open("/home/rnd1/data/twitter_crawler/twitter_stream/korean_tweet_streaming_keywords.json", "a", encoding='utf-8') as f:
                    #     json.dump(jsoned_data, f, ensure_ascii=False, indent=4)
                    #
                    # self.kor_tweet_count += 1
                # time.sleep(10)
                else:
                    # print("not ko")
                    # self.non_kor_tweet_count += 1
                    time.sleep(10)
                    return False
            else:
                time.sleep(10)
                return False

                # if not data.endswith('\n'):
                # 	self.current_file.write('\n')

    def on_error(self, status):
        print('Error:', str(status))
        logger.error(status)

        if str(status) == '420':
            sleepy = 60 * math.pow(2, self.siesta)
            print(time.strftime("%Y%m%d_%H%M%S"))
            print("A reconnection attempt will occur in " + \
                  str(sleepy / 60) + " minutes.")
            print('''
			*******************************************************************
			From Twitter Streaming API Documentation
			420: Rate Limited
			The client has connected too frequently. For example, an
			endpoint returns this status if:
			- A client makes too many login attempts in a short period
			of time.
			- Too many copies of an application attempt to authenticate
			with the same credentials.
			*******************************************************************
			''')
            time.sleep(sleepy)
            self.siesta += 1
        else:
            sleepy = 5 * math.pow(2, self.nightnight)
            print(time.strftime("%Y%m%d_%H%M%S"))
            print("A reconnection attempt will occur in " + \
                  str(sleepy) + " seconds.")
            time.sleep(sleepy)
            self.nightnight += 1
        return True

    def startFile(self):
        if self.current_file:
            self.current_file.close()

        local_time_obj = localtime()
        datetime = strftime("%Y_%m_%d_%H_%M_%S", local_time_obj)
        year = strftime("%Y", local_time_obj)
        month = strftime("%m", local_time_obj)

        full_path = os.path.join(self.path, year)
        full_path = os.path.join(full_path, month)
        try:
            os.makedirs(full_path)
            logger.info('Created %s' % full_path)
        except:
            pass
        filename = os.path.join(full_path, '%s.gz' % datetime)
        self.current_file = gzip.open(filename, 'w')
        self.file_start_time = time.time()
        logger.info('Starting new file: %s' % filename)


def loadStreamFile(stream_filename, stream_type):
    with open(stream_filename, 'r') as file:
        lines = file.readlines()

    content = '\n'.join(lines)
    index = content.find('=')
    if index != -1:
        content = content[index + 1:]
    return_val = content.split(',')

    if stream_type.lower() == 'location':
        for ii in range(len(return_val)):
            return_val[ii] = float(return_val[ii])

    return return_val


if __name__ == '__main__':
    options = [
        ['consumer_key=', 'The consumer key.', True, None],
        ['consumer_secret=', 'The consumer key secret.', True, None],
        ['access_token=', 'The access token.', True, None],
        ['access_token_secret=', 'The access token secret.', True, None],

        ['stream_type=', 'The type of stream to run: sample, location, keyword.', True, None],
        ['output_directory=', 'Where to save output files.', True, None],
        ['stream_filename=',
         'The name of the file containing parameters for this stream. Required for location and keyword.', False, ''],
        ['pid_file=', 'Save the pid of this job to the given file.', False, None],
        ['log_filename=', 'The log file.', True, None],
    ]
    # Start main method here.

    command_line = '%s'
    options_hash, remainder = parseCommandLine(options, command_line=command_line)

    if (len(remainder) != 0):
        print(usage(sys.argv, command_line, options))
        sys.exit()

    consumer_key = options_hash['consumer_key']
    consumer_secret = options_hash['consumer_secret']
    access_token = options_hash['access_token']
    access_token_secret = options_hash['access_token_secret']

    output_directory = options_hash['output_directory']

    stream_type = options_hash['stream_type']
    stream_filename = options_hash.setdefault('stream_filename', None)

    log_filename = options_hash['log_filename']
    logger = logging.getLogger('tweepy_streaming')
    handler = logging.FileHandler(log_filename, mode='a')
    formatter = logging.Formatter(fmt='%(asctime)s %(levelname)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

    # if stream_type.lower() == 'location' or stream_type.lower() == 'keyword':
    # stream_args = loadStreamFile(stream_filename, stream_type)

    if 'pid_file' in options_hash:
        pid_file = options_hash['pid_file']
        file = open(pid_file, 'w')
        file.write(str(os.getpid()))
        file.close()

    listener = FileListener(output_directory, 14400)
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    track_keywords = []

    with open("/home/alan/work/crawling_python_scripts/ko_stop_words_400.txt", "r", encoding='utf-8') as f:
        for line in f:
            line = line.replace('\n', '')
            track_keywords.append(line)

    try:
        while True:
            try:
                logger.warning("Connecting")
                stream = Stream(auth, listener)
                if stream_type.lower() == 'location':
                    stream.filter(locations=stream_args)
                elif stream_type.lower() == 'keyword':
                    # stream.filter(track=stream_args)
                    stream.filter(track=track_keywords, languages=['ko'])
                elif stream_type.lower() == 'sample':
                    stream.sample()
                else:
                    logger.error('Unknow stream type: ', twitterStream)
                    break
            # except IncompleteRead as e:
            except Exception as e:
                logger.error('Exception: ' + str(e))
    except Exception as e:
        logger.error('Exception: ' + str(e))
    logger.info('Exiting.')
