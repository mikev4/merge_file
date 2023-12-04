from multiprocessing import Pool, cpu_count
import os
import time
import read_and_write as rw
import sys
import split

class Worker:
    def __init__(self, process_size, file_path, split_count:int, split_num:int):
        self.process_size = process_size
        self.file_path = file_path
        self.split_count = split_count
        self.split_num = split_num
    
    def map(self, i):
        in_file_path = self.file_path + str(i)
        out_file_path = self.file_path + "out_"+ str(i)
        print("in_file_path: %s, out_file_path: %s, count: %s"
              % (in_file_path, out_file_path,  self.split_count))
        rw.read_and_write(in_file_path, out_file_path, int(self.split_count))
    
    def reduce(self):
        # todo: 合并所有的小文件
        print("合并所有的小文件")
        print("清理中间文件")
        
    def split():
        # todo: 拆分文件
        print("拆分文件")
        
    
    def execute(self):
        print('当前父进程: {}'.format(os.getpid()))
        self.split()
        start = time.time()
        p = Pool(int(self.process_size))
        for i in range(int(self.split_num)):
            p.apply_async(self.map, args=(i,))
        
        print('等待所有子进程完成。')
        p.close()
        p.join()
        self.reduce()
        end = time.time()
        print("总共用时{}秒".format((end - start)))           
    
if __name__=='__main__':
    if len(sys.argv) == 5:
        process_size = sys.argv[1]
        file_prefix = sys.argv[2]
        split_count = sys.argv[3]
        split_num = sys.argv[4]
        print("process_size: %s, file_prefix: %s, split_count: %s ,split_num: %s" % 
              (process_size, file_prefix, split_count, split_num))
        
        worker = Worker(process_size, file_prefix, split_count, split_num)
        worker.execute()
    else:
        print("len: %s" % len(sys.argv))
        print("""
                Usage: python mutiple_process_task.py [process_size] [file_prefix] [split_count]
                Example: python mutiple_process_task.py 4 /Users/guoqiang04/codes/python/dataprocess/data/wx.jsonl_part_
              """)

    