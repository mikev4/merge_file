from multiprocessing import Pool, cpu_count
import os
import time
import json
import sys
import logging
import jsonlines
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

class Worker:
    def __init__(self, process_size:int, file_path:str, split_lines_count:int):
        self.process_size = process_size
        self.file_path = file_path
        self.split_lines_count = split_lines_count
    
    def map(self, i):
        # /Users/guoqiang04/codes/python/dataprocess/data/wx.jsonl
        in_file_path = self.file_path +"_part_"+ str(i)
        out_file_path = self.file_path +"_part_out_"+ str(i)
        logging.info("in_file_path: %s, out_file_path: %s, count: %s"
              % (in_file_path, out_file_path,  self.split_lines_count))
        self.read_and_write(in_file_path, out_file_path, int(self.split_lines_count))
    
    def reduce(self):
        # todo: 合并所有的小文件
        merge_cmd = "cat " + self.file_path + "_part_out_* > " + self.file_path + "_out"
        logging.info("合并所有的小文件: %s" % (merge_cmd))
        os.system(merge_cmd)
        
        clean_cmd= "rm " + self.file_path + "_part_*"
        logging.info("清理中间文件: %s"%(clean_cmd))
        os.system(clean_cmd)
    
    def split(self):
        logging.info("拆分文件")
        split_num = self.split_file_by_line(self.file_path, int(self.split_lines_count))
        self.split_num = split_num
    
    def execute(self):
        logging.info('当前父进程: {}'.format(os.getpid()))
        self.split()
        start = time.time()
        p = Pool(int(self.process_size))
        for i in range(int(self.split_num)):
            p.apply_async(self.map, args=(i,))
        
        logging.info('等待所有子进程完成。')
        p.close()
        p.join()
        self.reduce()
        end = time.time()
        logging.info("总共用时{}秒".format((end - start))) 
    
    """
    将指定文件按指定行数分割成若干文件。

    Args:
        file_name (str): 待分割的文件名。
        split_lines (int): 每个分割文件包含的行数。

    Returns:
        分割后文件的数量。
    """
    @staticmethod
    def split_file_by_line(file_name:str, split_lines:int):
        split_files = []
        file_idx = 0
        line_ct = 0
        with open(file_name, 'r', encoding='utf-8') as fin:
            fout = None
            try:
                for line in fin:
                    if line_ct == 0:
                        if fout:
                            fout.close()  # 关闭上一个输出流
                        part_file = file_name + '_part_' + str(file_idx)
                        fout = open(part_file, 'w', encoding='utf-8')
                        split_files.append(part_file)
                    fout.write(line)
                    line_ct += 1
                    if line_ct >= split_lines:
                        line_ct = 0
                        file_idx += 1
                if fout:
                    fout.close()  # 确保最后一个输出流也被关闭
            except Exception as e:
                logging.info(f"Error occurred: {e}")
                if fout:
                    fout.close()
        logging.info(f'file: {file_name}, split lines: {split_lines}, split files num: {len(split_files)}')
        return len(split_files)          
 
    def read_and_write(self, in_path: str, out_path: str, total_count: int = 0):
        errs = 0
        cors = 0
        idx = 0
        with jsonlines.open(out_path, 'w') as writer:
            with open(in_path, 'r') as reader:
                for line in jsonlines.Reader(reader):
                    idx += 1
                    if (total_count > 0) and (idx % 100 == 0):
                        logging.info('file: {}, idx: {}, total: {}, ratio: {}%'
                                .format(in_path, idx, total_count, round(idx*100/total_count), 2))
                    if type(line) is not dict:
                        errs += 1
                    else:
                        try:
                            parsed_json = self.parse_json(line)
                            writer.write(parsed_json)
                        except Exception as err:
                            errs += 1
                            logging.ERROR(err)
                        else:
                            cors += 1

        logging.info('pid: {}, file: {}, errs: {}, cors: {} 100%.'.format(
            os.getpid(), in_path, errs, cors))
    
    def parse_json(self, line: str):
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
    
    
if __name__=='__main__':
    if len(sys.argv) == 4:
        process_size = sys.argv[1]
        file_path = sys.argv[2]
        split_lines_count = sys.argv[3]
        print("process_size: %s, file_prefix: %s, split_count: %s" % 
              (process_size, file_path, split_lines_count))
        
        worker = Worker(process_size, file_path, split_lines_count)
        worker.execute()
    else:
        print("len: %s" % len(sys.argv))
        print("""
                Usage: python mutiple_process_task.py [process_size] [file_path] [split_lines_count]
                Example: python mutiple_process_task.py 4 /Users/guoqiang04/codes/python/dataprocess/data/wx.jsonl 10000
              """)

    