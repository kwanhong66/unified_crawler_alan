import happybase
import json
import glob
import os
import logging
from datetime import datetime, timedelta
import pytz
from urllib.parse import urlparse, parse_qs

import configparser

logger = logging.getLogger('hbase_batch_insert_logger')
logger_fileHandler = logging.FileHandler('/home/rnd1/PycharmProjects/unified_crawler/logs/hbase_batch_insert.log')
logger_streamHandler = logging.StreamHandler()

logger.addHandler(logger_fileHandler)
logger.addHandler(logger_streamHandler)

logger.setLevel(logging.DEBUG)

get_today = lambda : datetime.now() + timedelta(hours=9)

def connect_to_hbase(crawler_name):
    """ Connect to HBase server.
      This will use the host, namespace, namespace_separator, timeout and batch size
      as defined in the hbase_config.cfg
    """
    config = configparser.ConfigParser()
    config.read('/home/rnd1/PycharmProjects/unified_crawler/hbase_config.cfg')

    host = config.get('ConnectionSetting', 'host')
    port = int(config.get('ConnectionSetting', 'port'))
    namespace = config.get('ConnectionSetting', 'table_prefix')
    namespace_separator = config.get('ConnectionSetting', 'table_prefix_separator')
    timeout = int(config.get('ConnectionSetting', 'timeout'))
    batch_size = int(config.get('TableSetting', 'batch_size'))

    table_name = ''

    if (crawler_name == 'naver-blog-crawler'):
        table_name = config.get('NaverBlogTableSetting', 'table_name')
    elif (crawler_name == 'naver-news-crawler'):
        table_name = config.get('NaverNewsTableSetting', 'table_name')
    elif (crawler_name == 'twitter-crawler'):
        table_name = config.get('TwitterStreamTableSetting', 'table_name')

    conn = happybase.Connection(host=host, port=port, table_prefix=namespace, table_prefix_separator=namespace_separator, timeout=timeout)
    conn.open()
    logger.info(u"Connect to HBase. host: {0:s}, port: {1:d}, table name: {2:s}, batch size: {3:d}" \
                .format(host, port, table_name, batch_size))

    # get table_name table object
    # on the assumption that the table is already created
    table = conn.table(table_name)

    # depending on time out parameter, certain batch size raise timed out exception (batch size : 1000)
    batch = table.batch(batch_size=batch_size)

    # transaction manner batch vs batch size limited
    # batch = table.batch(transaction=True)
    return conn, batch

def file_read(filename):
   json_data = open(filename)

   try:
       data = json.load(json_data)
   except ValueError as e:
       print('invalid json: %s' % filename)
       logger.error('invalid json: %s' % filename)
       data = None
   return data

def make_hbase_row(crawler_name, batch_file):
    rowkey = ''
    families = {}
    if (crawler_name == 'naver-blog-crawler'):
        raw_json = batch_file
        rowkey = batch_file['url']
        text = batch_file['text']
        written_time_temp = batch_file['writtenTime']
        date_str = list(written_time_temp)
        date_str[10] = ''
        written_time = ''.join(date_str)
        category_seq = batch_file['directorySeq']
        families = {
            'cf1_raw_data:raw_data' : json.dumps(raw_json, ensure_ascii=False),
            'cf2_text:text' : text,
            'cf3_written_time:written_time': written_time,
            'cf4_category_seq:category_seq': str(category_seq)
        }
    elif (crawler_name == 'naver-news-crawler'):
        raw_json = batch_file
        rowkey = batch_file['url']
        data = parse_qs(urlparse(rowkey).query, keep_blank_values=True)
        sid1 = data['sid1'][0]
        sid2 = data['sid2'][0]
        text = batch_file['content']
        written_time = batch_file['datetime'] # YYYY-MM-DD HH:mm:ss
        families = {
            'cf1_raw_data:raw_data': json.dumps(raw_json, ensure_ascii=False),
            'cf2_text:text': text,
            'cf3_written_time:written_time': written_time,
            'cf4_category:category': sid1
        }
    elif (crawler_name == 'twitter-crawler'):
        raw_json = batch_file
        rowkey = batch_file['id_str'] # tweet id
        text = batch_file['text']
        uid = batch_file['user']['id_str'] # user id
        d = datetime.strptime(batch_file['created_at'], '%a %b %d %H:%M:%S +0000 %Y').replace(tzinfo=pytz.UTC)
        written_time = d.strftime('%Y-%m-%d %H:%M:%S')  # YYYY-MM-DD HH:mm:ss
        category = batch_file['category']
        families = {
            'cf1_raw_data:raw_data': json.dumps(raw_json, ensure_ascii=False),
            'cf2_text:text': text,
            'cf3_written_time:written_time': written_time,
            'cf4_user_id:user_id' : uid,
            'cf5_category:category': category
        }

    return rowkey, families

def batch_insert(conn, batch, batch_files):
    crawler_name = batch_files[0]
    batch_files.pop(0)
    # print("Start to insert batch...")
    logger.info("Start to insert batch...")
    try:
        with batch as b:
            for batch_file in batch_files:
                key, families = make_hbase_row(crawler_name, batch_file)
                b.put(key, families)
    except Exception as e:
        # print ('Exception!!!')
        # print (e)
        logger.error("Excetption %s" % e)
    else:
        # print('batch insert done')
        logger.info("Batch insert done")
    finally:
        conn.close()
        logger.info("Connection closed")


def get_json_files_from_archive(basedir, seconddir, date):
    crawler_name = seconddir
    # YYYY-mm-dd
    date_path = '%s/%02d/%02d' % (int(date[0:4]), int(date[5:7]), int(date[8:10]))
    thirddir = ''

    item_list = []
    item_list.append(crawler_name)

    if (crawler_name == 'naver-blog-crawler'):
        thirddir = 'blog_text'
    elif (crawler_name == 'naver-news-crawler'):
        thirddir = 'news_text'
    elif (crawler_name == 'twitter-crawler'):
        thirddir = 'twitter_stream'
    else:
        raise Exception()


    base_targetpath = '%s/%s/%s'\
                         % (basedir, crawler_name, thirddir)
    subdir_list = os.listdir(base_targetpath)

    for subdir in subdir_list:
        full_path = os.path.join(base_targetpath, subdir, date_path)

        filenames = glob.glob('%s/*.json' % full_path)

        # print(len(filenames))

        # for each json file, multiple json objects could be included,
        # for example, news. If needed, the way of archiving json files in crawling code
        # needs to be modified to below code can be simplified.
        for filename in filenames:
            if (crawler_name == 'naver-blog-crawler'):
                item = file_read(filename)
                if (item != None):
                    item_list.append(item)
                else:
                    continue
            else:
                items = file_read(filename)
                if (items != None):
                    for item in items:
                        item_list.append(item)
                else:
                    continue

    # print(item_list[0])
    logger.info("Retrieving archived files is done. the number of file(json): %i" % len(item_list))
    return item_list


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Get input parameters.',
                        formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('-p', '--path', dest='basedir',
                         help='assign data path')
    parser.add_argument('-d', '--date', dest='date',
                         help='assign date to crawl')
    parser.add_argument('-sdir', '--seconddir', dest='seconddir',
                         help='assign crawler name')
    args = parser.parse_args()

    if not args.basedir:
        args.basedir = '/home/rnd1/data'

    logger.info("HBase batch insert start... %s" % (get_today().strftime("%Y-%m-%d %H:%M")))
    # print('Connecting to HBase...')
    conn, batch = connect_to_hbase(args.seconddir)
    # print('Retrieving archived files...')
    batch_files = get_json_files_from_archive(args.basedir, args.seconddir, args.date)
    batch_insert(conn, batch, batch_files)
