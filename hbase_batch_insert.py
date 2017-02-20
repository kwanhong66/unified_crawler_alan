import happybase
import json
import glob
import os

import configparser

def connect_to_hbase():
    config = configparser.ConfigParser()
    config.read("hbase_config.cfg")

    host = config.get('ConnectionSetting', 'host')
    port = int(config.get('ConnectionSetting', 'port'))
    namespace = config.get('ConnectionSetting', 'table_prefix')
    namespace_separator = config.get('ConnectionSetting', 'table_prefix_separator')
    batch_size = int(config.get('TableSetting', 'batch_size'))

    table_name = config.get('NaverBlogTableSetting', 'table_name')

    conn = happybase.Connection(host=host, port=port, table_prefix=namespace, table_prefix_separator=namespace_separator)
    conn.open()

    # get table_name table object
    # on the assumption that the table is already created
    table = conn.table(table_name)
    # depending on time out parameter, certain batch size raise timed out exception (batch size : 1000)
    batch = table.batch(batch_size=batch_size)
    # batch = table.batch()
    return conn, batch

def file_read(filename):
   json_data = open(filename)
   data = json.load(json_data)
   return data

def make_hbase_row(crawler_name, batch_file):
    rowkey = ''
    families = {}
    if (crawler_name == 'naver-blog-crawler'):
        raw_json = batch_file
        rowkey = batch_file['url']
        text = batch_file['text']
        crawled_time = batch_file['crawledTime']
        families = {
            'cf1:raw_data' : json.dumps(raw_json),
            'cf2:text' : text,
            'cf3:crawled_time': crawled_time
        }
    elif (crawler_name == 'naver-news-crawler'):
        raw_json = batch_file
        rowkey = batch_file['url']
        text = batch_file['content']
        written_time = batch_file['datetime'] # YYYY-MM-DD HH:mm:ss
        families = {
            'cf1:raw_data': json.dumps(raw_json),
            'cf2:text': text,
            'cf3:crawled_time': written_time
        }
    elif (crawler_name == 'twitter-crawler'):
        thirddir = 'twitter_stream'

    return rowkey, families

def return_information(directory_seq, basedir, date, seconddir, thirddir):
    directory_seq = int(directory_seq)
    try:
        targetpath = '%s/%s/%s/%02d/%s/%02d/%02d'\
                         % (basedir, seconddir, thirddir, directory_seq,\
                            int(date[0:4]), int(date[5:7]), int(date[8:10]))
    except TypeError as e:
        print (e)
        raise Exception('Please check input values (ex: the date)')
    json_list = []
    filenames = glob.glob('%s/*.json' % targetpath)
    for filename in filenames:
        item = file_read(filename)
        json_list.append(item)
    return json_list

def batch_insert(conn, batch, batch_files):
    crawler_name = batch_files[0]
    batch_files.pop(0)
    print("Start to insert batch...")
    try:
        with batch as b:
            for batch_file in batch_files:
                key, families = make_hbase_row(crawler_name, batch_file)
                b.put(key, families)
    except Exception as e:
        print ('Exception!!!')
        # print(e.with_traceback(0))
        print (e)
    finally:
        conn.close()
    print ('batch insert done')

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
        # has to be modified to below code can be simplified.
        for filename in filenames:
            if (crawler_name == 'naver-blog-crawler'):
                item = file_read(filename)
                item_list.append(item)
            else:
                items = file_read(filename)
                for item in items:
                    item_list.append(item)

    print(item_list[0])
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

    conn, batch = connect_to_hbase()
    print ('Connect to HBase.')
    batch_files = get_json_files_from_archive(args.basedir, args.seconddir, args.date)
    # batch_insert(conn, batch, batch_files)

