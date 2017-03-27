#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
twitter_streamer.py:

KeywordsStreamer: straightforward class that tracks a list of keywords; most of the jobs are done by TwythonStreamer; the only thing this is just attach a WriteToHandler so results will be saved

'''

import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

requests_log = logging.getLogger("requests")
requests_log.setLevel(logging.WARNING)

import sys, time, argparse, json, os, pprint, datetime
import twython
import math
# from util import full_stack, chunks, md5


class TwitterStreamer(twython.TwythonStreamer):
    def __init__(self, APP_KEY, APP_SECRET, OAUTH_TOKEN, OAUTH_TOKEN_SECRET, output_folder='./data', category=None):
        self.siesta = 0
        self.nightnight = 0

        self.output_folder = os.path.abspath(output_folder)
        if not os.path.exists(self.output_folder):
            os.makedirs(self.output_folder)

        self.item_list = []

        self.counter = 1
        if category is not None:
            self.category = category

        super(TwitterStreamer, self).__init__(APP_KEY, APP_SECRET, OAUTH_TOKEN, OAUTH_TOKEN_SECRET)

    def on_success(self, tweet):

        if 'text' in tweet:
            now = datetime.datetime.now() + datetime.timedelta(hours=9) # server time is set utc
            if self.category is None:
                target_path = '%s/%s/%s/%s/' \
                            % (self.output_folder, now.strftime('%Y'), now.strftime('%m'), now.strftime('%d'))
            else:
                target_path = '%s/%s/%s/%s/%s/' \
                              % (self.output_folder, self.category, now.strftime('%Y'), now.strftime('%m'), now.strftime('%d'))
            # month_folder = os.path.abspath('%s/%s' % (self.output_folder, now.strftime('%Y-%m')))
            if not os.path.exists(target_path):
                os.makedirs(target_path)

            tweet['category'] = self.category

            if (self.counter % 100 != 0):
                self.item_list.append(tweet)
                # print(self.counter)
                self.counter += 1
            else:
                # output_file = os.path.abspath('%s/%s/%d.json' % (target_path, now.strftime("%Y%m%d"), (self.counter % 1000)))
                file_name = now.strftime("%Y%m%d") + '_' + str((self.counter // 100)) + '.json'
                output_path = os.path.join(target_path, file_name)
                self.counter += 1
                with open(output_path, 'w+') as f:
                    # f.write('%s\n' % json.dumps(tweet))
                    # print('Writing...', output_path)
                    json.dump(self.item_list, f, ensure_ascii=False, indent=4)
                    del self.item_list[:]
                    # self.counter += 1
                    # if self.counter % 1000 == 0:
                    #     logger.info("received: %d" % self.counter)

    def on_error(self, status_code, data):
        logger.warn('ERROR CODE: [%s]-[%s]' % (status_code, data))
        if status_code == '420':
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

    def close(self):
        self.disconnect()


def init_streamer(config, output_folder, category=None):
    apikeys = list(config['apikeys'].values()).pop()

    APP_KEY = apikeys['app_key']
    APP_SECRET = apikeys['app_secret']
    OAUTH_TOKEN = apikeys['oauth_token']
    OAUTH_TOKEN_SECRET = apikeys['oauth_token_secret']
    if category is None:
        streamer = TwitterStreamer(APP_KEY, APP_SECRET, OAUTH_TOKEN, OAUTH_TOKEN_SECRET, output_folder=output_folder)
    else:
        streamer = TwitterStreamer(APP_KEY, APP_SECRET, OAUTH_TOKEN, OAUTH_TOKEN_SECRET, output_folder=output_folder, category=category)
    return streamer


def collect_public_tweets(config, output_folder):
    streamer = init_streamer(config, output_folder)

    logger.info("start collecting.....")

    streamer.statuses.sample()


def filter_by_locations(config, output_folder, locations=None):
    with open(os.path.abspath(locations), 'r') as locations_f:
        geo_locations = json.load(locations_f)

        name = geo_locations['name']
        locations = geo_locations['locations']

        streamer = init_streamer(config, '%s/%s' % (output_folder, name))

        logger.info("start collecting for %s....." % (name))

        # if (locations and locations.endswith('.json')):
        #     with open(os.path.abspath(locations), 'r') as locations_f:
        #         locations = json.load(locations_f)
        #         locations = ','.join([','.join([str(g) for g in pair]) for pair in locations['bounding_box']])

        streamer.statuses.filter(locations=locations)

def filter_by_keywords(config, output_folder, category=None):
    with open(os.path.abspath('/home/rnd1/PycharmProjects/unified_crawler/twitter_stream/track_keywords.json'), 'r') as keywords_f:
        category_keywords = json.load(keywords_f)
        category_key = category
        # load pre-defined category keywords from track_keywords.json
        keywords_list = category_keywords[category_key]

        streamer = init_streamer(config, output_folder, category=category)
        # keywords_list = ["가","가까스로","가령","각","각각"]
        streamer.statuses.filter(track=keywords_list)

def filter_by_users(config, output_folder, category=None):
    with open(os.path.abspath('/home/rnd1/PycharmProjects/unified_crawler/twitter_stream/track_users.json'), 'r') as users_f:
        user_categories = json.load(users_f)
        category_key = category
        # load pre-defined category users from track_users.json
        users_list = user_categories[category_key]

        streamer = init_streamer(config, output_folder, category=category)

        streamer.statuses.filter(follow=users_list)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', help="config.json that contains twitter api keys;", default="/home/rnd1/PycharmProjects/unified_crawler/twitter_stream/twitter_app_auth.json")
    parser.add_argument('-o', '--output', help="output folder data", default="/home/rnd1/data/twitter_crawler/twitter_stream/")
    parser.add_argument('-cmd', '--command', help="command", default="keywords")
    parser.add_argument('-cc', '--command_data', help="command data", default=None)

    args = parser.parse_args()

    with open(os.path.abspath(args.config), 'r') as config_f:
        config = json.load(config_f)

        try:
            while (True):
                try:
                    if (args.command == 'locations'):
                        filter_by_locations(config, args.output, args.command_data)
                    elif (args.command == 'keywords'):
                        filter_by_keywords(config, args.output, args.command_data)
                    elif (args.command == 'users'):
                        filter_by_users(config, args.output, args.command_data)
                    else:
                        collect_public_tweets(config, args.output)
                except Exception as exc:
                    logger.error(exc)
                    # logger.error(full_stack())
                time.sleep(10)
                logger.info("restarting...")
        except KeyboardInterrupt:
            print()
            logger.error('You pressed Ctrl+C!')
            pass
        finally:
            pass
