import logging
import time
import os

from jsonpath_rw import jsonpath,parse

# LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
# DATE_FORMAT = "%m/%d/%Y %H:%M:%S %p"
# logging.basicConfig(filename='./test_'+time.strftime("%Y-%m-%d", time.localtime())+'.log', level=logging.DEBUG, format=LOG_FORMAT, datefmt=DATE_FORMAT)
# msg = time.strftime("%Y-%m-%d %H:%m:%s", time.localtime())
# logging.info(msg)

#
#
# jsonpath_expr = parse('foo[*].baz')
# data = {'foo': [{'baz': 'news'}, {'baz': 'music'}]}
# print([match.value for match in jsonpath_expr.find(data)][0])


# mage_chapters_list_file_path = "/data1/test/test.txt"
# print(os.access(mage_chapters_list_file_path, os.R_OK | os.W_OK | os.X_OK))


#
# num_str_start_symbol = ['一', '二', '两', '三', '四', '五', '六', '七', '八', '九', '十']
# more_num_str_symbol = ['零', '一', '二', '两', '三', '四', '五', '六', '七', '八', '九', '十', '百', '千', '万', '亿']
# common_used_numerals_tmp = {'零': 0, '一': 1, '二': 2, '两': 2, '三': 3, '四': 4, '五': 5, '六': 6, '七': 7, '八': 8, '九': 9,
#                             '十': 10, '百': 100, '千': 1000, '万': 10000, '亿': 100000000}
# common_used_numerals = {}
# for key in common_used_numerals_tmp:
#    common_used_numerals[key] = common_used_numerals_tmp[key]
#
#
#
#  """
#         大小写的转换的
#     """
# def chinese2digits(self,uchars_chinese):
#     total = 0
#     r = 1  # 表示单位：个十百千...
#     for i in range(len(uchars_chinese) - 1, -1, -1):
#         val = common_used_numerals.get(uchars_chinese[i])
#         if val >= 10 and i == 0:  # 应对 十三 十四 十*之类
#             if val > r:
#                 r = val
#                 total = total + val
#             else:
#                 r = r * val
#                 # total =total + r * x
#         elif val >= 10:
#             if val > r:
#                 r = val
#             else:
#                 r = r * val
#         else:
#             total = total + r * val
#     return total
#
# def changeChineseNumToArab(self,oriStr):
#     lenStr = len(oriStr);
#     aProStr = ''
#     if lenStr == 0:
#         return aProStr;
#
#     hasNumStart = False;
#     numberStr = ''
#     for idx in range(lenStr):
#         if oriStr[idx] in num_str_start_symbol:
#             if not hasNumStart:
#                 hasNumStart = True;
#
#             numberStr += oriStr[idx]
#         else:
#             if hasNumStart:
#                 if oriStr[idx] in more_num_str_symbol:
#                     numberStr += oriStr[idx]
#                     continue
#                 else:
#                     numResult = str(self.chinese2digits(numberStr))
#                     numberStr = ''
#                     hasNumStart = False;
#                     aProStr += numResult
#
#             aProStr += oriStr[idx]
#             pass
#
#     if len(numberStr) > 0:
#         resultNum = self.chinese2digits(numberStr)
#         aProStr += str(resultNum)
#
#     return aProStr

# class FilterTag(object):
#
#     def __init__(self, separator=u'zhangt'):
#         self.separator = separator
#
#     def __call__(self, values):
#         if self.separator==''
#             return re.sub(r'<[^>]*?>', '', self.separator.join(values.split()))
#
# map(FilterTag("xiaowen",))


ccc = [
    {"_id": 238, "name": "第248章 岁月静好（大结局）", "add_time": "2018-11-29 11:24:45"},
    {"_id": 238, "name": "第244章 岁月静好", "add_time": "2018-11-29 11:24:45"},
    {"_id": 238, "name": "21321", "add_time": "2018-11-29 11:24:45"},
    {"_id": 238, "name": "大结局", "add_time": "2018-11-29 11:24:45"},
    {"_id": 238, "name": "22", "add_time": "2018-11-29 11:24:45"},
]

aa = [ i for i in ccc if i['name']== "大结局"]

print(aa)



class FilterTag(object):

    def __init__(self, separator=u' '):
        self.separator = separator

    def __call__(self, values):
        if self.separator == values['name'].strip():
            return 1
        else:
            return 0

strss = "  第244章 岁月静好   jj"


print(any(map(FilterTag(strss.strip()),ccc)))
exit()

# str = "大盗贼$c泛舟填词"
#
# print(str.split('$c')[0])


# testStr = ['我是这个样的两百三十二', '我有两百三十二块钱', '十二个套餐', '一亿零八万零三百二十三', '今天天气真不错',
#            '百分之八十 discount rate很高了', '千万不要',
#            '这个invoice value值一百万',
#            '我的一百件商品have quality',
#            '找一找我的收藏夹里，有没有一个眼镜', ]
#
# for tstr in testStr:
#     print(tstr + ' = ' + changeChineseNumToArab(tstr))


