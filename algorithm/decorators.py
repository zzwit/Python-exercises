# -*- coding:utf-8 -*-
"""
@author:zzw
@file: decorators.py
@time: 2019/09/23
"""
import time

"""
装饰器，python装饰器（fuctional decorators）就是用于拓展原来函数功能的一种函数，目的是在不改变原函数名(或类名)的情况下，给函数增加新的功能。 
这个函数特殊之处，它返回也是一个函数，这个函数是内嵌函数"原"函数的函数
"""

#
# def f():
#     start_time = time.time()
#     print("hello")
#     time.sleep(1)
#     print("word")
#     end_time = time.time()
#     execution_time = (end_time - start_time) * 1000
#     print("time is %d ms" % execution_time)
#


#使用函数的，嵌套类另一个函数，如果多个函数来是，那麻烦了
# def deco(func):
#     start_time = time.time()
#     f()
#     end_time = time.time()
#     execution_time = (end_time - start_time) * 1000
#     print("time is %d ms" % execution_time)
# def f():
#     print("hello")
#     time.sleep(1)
#     print("word")

#扩展1：带有固定参数的装饰器
# def deco(f):
#     def wrapper(a,b):
#         start_time = time.time()
#         f(a,b)
#         end_time = time.time()
#         execution_time = (end_time - start_time)*1000
#         print("time is %d ms" % execution_time)
#     return wrapper
#
# @deco
# def f(a,b):
#     print("be on")
#     time.sleep(1)
#     print("result is %d" %(a+b))


# def deco(func):
#     def wrapper(*args, **kwargs):
#         print(args,"args")
#         start_time = time.time()
#         func(*args, **kwargs)
#         end_time = time.time()
#         execution_time = (end_time - start_time)*1000
#         print("time is %d ms" %execution_time)
#     return wrapper
#
# @deco
# def f(a,b):
#     print("be on")
#     time.sleep(1)
#     print("result is %d" %(a+b))
#
# @deco
# def f2(a,b,c):
#     print("be on")
#     time.sleep(1)
#     print("result is %d" %(a+b+c))
#
# if __name__ == '__main__':
#     f2(3, 4, 5)
#     f(3, 4)


"""
多个装饰器的函数使用 先执行的第一个装饰器在执装饰器，函数内容调用函数
"""
#
# def deco01(f):
#     def wrapper(*args, **kwargs):
#         print("this is deco01")
#         start_time = time.time()
#         f(*args, **kwargs)
#         end_time = time.time()
#         execution_time = (end_time - start_time)*1000
#         print("time is %d ms" % execution_time)
#         print("deco01 end here")
#     return wrapper
#
# def deco02(f):
#     def wrapper(*args, **kwargs):
#         print("this is deco02")
#         f(*args, **kwargs)
#
#         print("deco02 end here")
#     return wrapper
#
# @deco01
# @deco02
# def f(a,b):
#     print("be on")
#     time.sleep(1)
#     print("result is %d" %(a+b))
#
#
# if __name__ == '__main__':
#     f(3,4)



def wrapper_out1(func):
    print('--out11--')
    def inner1(*args, **kwargs):
        print("--in11--")
        ret = func(*args, **kwargs)
        print("--in12--")
        return ret
    print("--out12--")
    return inner1
def wrapper_out2(func):
    print('--out21--')
    def inner2(*args, **kwargs):
        print("--in21--")
        ret = func(*args, **kwargs)
        print("--in22--")
        return ret
    print("--out22")
    return inner2
@wrapper_out2
@wrapper_out1
def test():
    print("--test--")
    return 1 * 2
if __name__ == '__main__':
    test()































