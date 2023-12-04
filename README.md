## 使用说明
主要：切分文件、处理文件、合并文件
### 不切分文件直接处理
```
    python read_and_write.py [input_path] [output_path]
```
就是慢，应该不会内存溢出
### 切分文件
1. 先切分文件
```
    python split.py [input_file_path]
```
2. 切分出来的文件，并行处理
```
    python mutiple_process_task.py python mutiple_process_task.py [process_size] [file_prefix]
```
切出来的文件跟源文件放在同一个目录下