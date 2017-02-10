import happybase
import json
import glob

def file_read(filename):
   json_data = open(filename)
   data = json.load(json_data)
   return data


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

def batch_insert(batch_files):

    connection = happybase.Connection('localhost')
    connection.open()

    table_name = 'naver_blog_table'

    connection.create_table('naver_blog_table',{'cf_text':{}, 'cf_meta':{}}) # table existence check needed if table was not set up already
    table = connection.table(table_name)

    print("Start to insert batch...")
    try:
        with table.batch() as b:
            for item in batcth_files:
                text = item['text']
                title = item['title']
                url = item['url']
                written_time = item['writtenTime']
                crawled_time = item['crawledTime']
                category = item['directorySeq']
                b.put(url,
                      {
                          'cf_text:txt': text,
                          'cf_meta:title': title,
                          'cf_meta:written_time': written_time,
                          'cf_meta:crawled_time': crawled_time,
                          'cf_meta:category': category
                      })
    except Exception as e:
        print (e)

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Get input parameters.',
                        formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('-c', '--category', dest='directory_seq',
                         help='assign target category to crawl')
    parser.add_argument('-p', '--path', dest='basedir',
                         help='assign data path')
    parser.add_argument('-d', '--date', dest='date',
                         help='assign date to crawl')
    parser.add_argument('-sdir', '--seconddir', dest='seconddir',
                         help='assign crawler name')
    parser.add_argument('-tdir', '--thirddir', dest='thirddir',
                         help='assign text dir')
    args = parser.parse_args()

    if not args.basedir:
        args.basedir = '/home/rnd1/data'

    batcth_files = return_information(args.directory_seq, args.basedir, args.date, args.seconddir, args.thirddir)
    batch_insert(batch_files=batcth_files)