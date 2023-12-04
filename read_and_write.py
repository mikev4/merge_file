# coding:utf-8
from bs4 import BeautifulSoup
import json
import jsonlines
import sys
import os


def read_and_write(in_path: str, out_path: str, total_count: int = 0):
    errs = 0
    cors = 0
    idx = 0
    with jsonlines.open(out_path, 'w') as writer:
        with open(in_path, 'r') as reader:
            for line in jsonlines.Reader(reader):
                idx += 1
                if (total_count > 0) and (idx % 100 == 0):
                    print('file: {}, idx: {}, total: {}, ratio: {}%'
                            .format(in_path, idx, total_count, round(idx*100/total_count), 2))
                if type(line) is not dict:
                    errs += 1
                else:
                    try:
                        parsed_json = parse_json(line)
                        writer.write(parsed_json)
                    except Exception as err:
                        errs += 1
                        print(err)
                    else:
                        cors += 1

    print('pid: {}, file: {}, errs: {}, cors: {} 100%.'.format(
        os.getpid(), in_path, errs, cors))


def parse_json(line: str):
    # #start{ 代表 < ; #end} 代表 >
    start = "#start{"
    end = "#end}"
    htmlstr = line['news_content_label']
    soup = BeautifulSoup(htmlstr, 'html.parser')
    img_tags = soup.find_all('img')
    for img in img_tags:
        img.replace_with("%simg src='%s' %s" % (start, img.get('src'), end))
    news_content_label_format = soup.get_text(strip=True)
    news_content_label_format = news_content_label_format.replace(
        start, '<').replace(end, '>')
    line['news_content_label_format'] = news_content_label_format
    return line


if __name__ == "__main__":
    # in_path = 'dataprocess/data/total.jsonl_part_13'
    # out_path ='dataprocess/data/total.jsonl_part_13_cp'
    # total_count = 1250
    # read_and_write(in_path, out_path, int(total_count))
    
    if len(sys.argv) == 4:
        in_path = sys.argv[1]
        out_path = sys.argv[2]
        total_count = sys.argv[3]
        read_and_write(in_path, out_path, int(total_count))
    else:
        print("len: %s" % len(sys.argv))
        print(
            "Usage: python read_and_write.py [in_path] [out_path] [total_count]")
