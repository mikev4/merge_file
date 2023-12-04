import sys
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
    return split_files

if __name__ == '__main__':
    if len(sys.argv) == 3:
        file_path = sys.argv[1]
        split_num = sys.argv[2]
        split_file_by_line(file_path, int(split_num))    
    else:
        print("len: %s" % len(sys.argv))
        print("""
                Usage: python split.py [file_[path]] [split_num]
                Example: python split.py /Users/guoqiang04/codes/python/dataprocess/data/wx.jsonl 12000
              """)    