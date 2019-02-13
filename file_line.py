"""
 合并小说的内容
"""
file = open("228901.txt")
file1 =open("book_35_path.txt",'a+')
for line in file:
    line = line.strip('\n')
    #file1.write('rsync /data1/book/35/'+line+'/ /data/book/35/'+line+'/ &'+'\n')
    file1.write('/data/book/35/' + line + '/' + line + '\n')
file.close()
