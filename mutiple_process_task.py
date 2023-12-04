from multiprocessing import Pool, cpu_count
import os
import time
import read_and_write as rw
import sys

class Worker:
    def __init__(self, process_size:int, file_path:str, split_lines_count:int):
        self.process_size = process_size
        self.file_path = file_path
        self.split_lines_count = split_lines_count
    
    def map(self, i):
        # /Users/guoqiang04/codes/python/dataprocess/data/wx.jsonl
        in_file_path = self.file_path +"_part_"+ str(i)
        out_file_path = self.file_path +"_part_"+ "out_"+ str(i)
        print("in_file_path: %s, out_file_path: %s, count: %s"
              % (in_file_path, out_file_path,  self.split_lines_count))
        rw.read_and_write(in_file_path, out_file_path, int(self.split_lines_count))
    
    def reduce(self):
        # todo: 合并所有的小文件
        print("合并所有的小文件")
        print("清理中间文件")
        
    def split(self):
        print("拆分文件")
        split_num = self.split_file_by_line(self.file_path, int(self.split_lines_count))
        self.split_num = split_num
    
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
    
    """
    将指定文件按指定行数分割成若干文件。

    Args:
        file_name (str): 待分割的文件名。
        split_lines (int): 每个分割文件包含的行数。

    Returns:
        分割后文件的数量。
    """
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
                print(f"Error occurred: {e}")
                if fout:
                    fout.close()
        print(f'file: {file_name}, split lines: {split_lines}, split files num: {len(split_files)}')
        return len(split_files)          
    
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

    