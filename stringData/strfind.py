def findstr(str1,str2):
    if str2 not in str1:
        print("在目标字符串中没有找到子字符串！")
    else:
        print("子字符串在目标字符串中共出现的次数：",str1.count(str2))


str1 = input("请输入目标字符串：")
str2 = input("请输入子字符串（两个字符）：")
findstr(str1,str2)