1. 可出处理大文件大文件，内存不溢出
2. 数据清洗使用bs4
```
    # 第一个参数是进程数、可以按照核心数设置，
    # 第二个参数是文件路径
    # 第三个参数是文件多少行切割
    python mutiple_process_task.py 10 /Users/guoqiang04/codes/python/dataprocess/data/wx.jsonl 1000
```
