# -*-coding:utf-8-*-

import json
import os
import glob
# import urllib2
import urllib.request
import time

from bs4 import BeautifulSoup

from naver_blog_crawler.utils import checkdir, file_read, get_version

URLBASE = 'http://m.blog.naver.com/%s/%s'

def get_page(url):
    try:
        page = urllib.request.urlopen(url, timeout=3)
        doc  = BeautifulSoup(page.read(), "lxml")
        return (doc, doc.find("div", {"class": "_postView"}))
    except Exception as e:
        print (e, url)
        time.sleep(100)
        return None

def make_structure(blog_id, log_no, raw, doc, crawled_time,
                                    title, written_time, url, tags, directory_seq,
                                    encoding='utf-8'):

    sub_doc = doc.find("div", {"class": "post_ct"})
    texts = [text.get_text() for text in sub_doc.find_all("p", {})]
    # text = doc.find("p", {"class": "se_textarea"}).get_text()
    merged_text = ' '.join(texts)

    # extract_category     = lambda doc: doc.find("a", {"class": "_categoryName"}).get_text().encode(encoding)
    # extract_content_html = lambda doc: doc.find("div", {"id": "viewTypeSelector"})

    return {
            u"directorySeq": directory_seq,
            u"title": title,
            u"text": merged_text,
            u"writtenTime": written_time,
            u"crawledTIme": crawled_time,
            u"url": url,
            u"tags": tags
    }

def make_json(blog, blog_id, log_no, date, directory_seq, basedir, seconddir = "blog_text"):
    PATH = '%s/%02d/%02d' % (int(date[0:4]), int(date[5:7]), int(date[8:10]))
    targetpath = '%s/%s/%02d/%s' % (basedir, seconddir, directory_seq, PATH)
    checkdir(targetpath)
    filename = '%s/%s.json' % (targetpath, log_no)
    f        = open(filename, 'w')
    jsonstr  = json.dumps(blog, sort_keys=True, indent=4, ensure_ascii=False)
    f.write(jsonstr)
    f.close()

def merge_jsons(directory_seq, basedir, date, thirddir="blog_text"):
    directory_seq = int(directory_seq)
    targetpath = '%s/%s/%02d/%s/%02d/%02d' \
                 % (basedir, thirddir, directory_seq, \
                    int(date[0:4]), int(date[5:7]), int(date[8:10]))
    file_path = os.path.join(targetpath, 'merged.json')
    item_list = []
    filenames = glob.glob('%s/*.json' % targetpath)

    for filename in filenames:
        item = file_read(filename)
        item_list.append(item)

    with open(file_path, "w") as outfile:
        json.dump(item_list, outfile, sort_keys=True, indent=4, ensure_ascii=False)

    print("merge file is created")

def error_log_url(blog_id, log_no, date, directory_seq, basedir, seconddir = "blog_text", thirddir = "logs"):
    targetpath = '%s/%s/%s' % (basedir, seconddir, thirddir)
    checkdir(targetpath)
    filename = '%s/error_url_%s-%02d-%02d.txt' % (targetpath, int(date[0:4]), int(date[5:7]), int(date[8:10]))
    f   = open(filename, 'a')
    url = '%s, http://m.blog.naver.com/%s/%s, access denied\n' % (directory_seq, blog_id, log_no)
    f.write(url)
    f.close()

def web_crawl(blog_id, log_no, crawled_time, title,
                        written_time, url, tags, date, directory_seq, basedir, debug=False):
    url = URLBASE % (blog_id, log_no)
    (raw, doc) = get_page(url)
    if doc != None:
        blog = make_structure(blog_id, log_no, raw, doc, crawled_time,
                        title, written_time, url, tags, directory_seq)
        make_json(blog, blog_id, log_no, date, directory_seq, basedir)
    else:
        error_log_url(blog_id, log_no, date, directory_seq, basedir)

def return_information(directory_seq, basedir, date, seconddir ="url_list", thirddir="blog_text", debug=False):
    if debug:
        print ("Start blog text crawling...")
    directory_seq = int(directory_seq)
    try:
        targetpath = '%s/%s/%02d/%s/%02d/%02d'\
                         % (basedir, seconddir, directory_seq,\
                            int(date[0:4]), int(date[5:7]), int(date[8:10]))
    except TypeError as e:
        print (e)
        raise Exception('Please check input values (ex: the date)')
    itr1 = 0
    filenames = glob.glob('%s/*.json' % targetpath)
    for filename in filenames:
        print (filename)
        items = file_read(filename)
        itr2 = 0
        for i, blog in enumerate(items):
            try:
                check_targetpath = '%s/%s/%02d/%s/%02d/%02d'\
                                % (basedir, thirddir, directory_seq,\
                                   int(date[0:4]), int(date[5:7]), int(date[8:10]))
                check_filename = '%s.json' % (items[i]['logNo'])
                if not os.path.isfile('%s/%s' % (check_targetpath, check_filename)):
                    web_crawl(items[i]['blogId'],
                              items[i]['logNo'],
                              items[i]['crawledTime'],
                              items[i]['title'],
                              items[i]['writtenTime'],
                              items[i]['url'],
                              items[i]['tags'],
                              date,
                              directory_seq,
                              basedir, debug=debug)
            except Exception as e:
                print (e)
            itr2 += 1
        if itr2 == len(items):
            print ("%s items read completed successfully." % len(items))
        else:
            print ("Not all items read.")
        itr1 += 1
    if len(filenames) == itr1:
        print ("%s files read completed successfully." % len(filenames))
        if len(filenames)==0:
            print ("You probably have to crawl lists first.")
    else:
        print ("Not all files read.")

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Get input parameters.',
                        formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('-c', '--category', required=True, dest='directory_seq',
                         help='assign target category to crawl')
    parser.add_argument('-p', '--path', dest='basedir',
                         help='assign data path')
    parser.add_argument('-d', '--date', dest='date',
                         help='assign date to crawl')
    parser.add_argument('--debug', dest='debug', action='store_true',
                         help='enable debug mode')
    args = parser.parse_args()

    if not args.basedir:
        args.basedir = './data'

    if args.debug:
        debug = True
    else:
        debug = False

    return_information(args.directory_seq, args.basedir, args.date, debug=debug)
    # merge_jsons(args.directory_seq, args.basedir, args.date)