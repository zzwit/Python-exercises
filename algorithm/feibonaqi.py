# -*- coding:utf-8 -*-
"""
@author:zzw
@file: feibonaqi.py
@time: 2019/09/23
- 斐波那契 算法
"""
import time


# 第一版 执行 1.5s
# def fib(n):
#     if n == 1 or n == 0:
#       return 1
#     else:
#       return fib(n - 2) + fib(n - 1)

# 优化的1 将数值都缓存的内存中直接返回 执行优化 1.0s
# def fib(n):
#     count = 0
#     if n == 1 or n == 0:
#       count =1
#     else:
#       count = fib(n - 2) + fib(n - 1)
#     return count

# # 优化2，将数值存在字典中 执行 0.0013689994812011719
# def fib2(n, cache=None):
#     if cache is None:
#         # 不存在创建一个新字典
#         cache = {}
#     if n == 1 or n == 0:
#         return 1
#     if n in cache:
#         return cache[n]
#     else:
#         cache[n] = fib2(n - 2, cache) + fib2(n - 1, cache)
#
#     return cache[n]

# 装饰器
def decorate(func):
    cache = {}

    def wrap(*args):
        print(args, "args")
        if args not in cache:
            cache[args] = func(*args)

        print(cache,"cache-----cache")
        return cache[args]

    return wrap


# @decorate
# def fib2(n):
#     if n == 1 or n == 0:
#         return 1
#     else:
#         return fib2(n - 2) + fib2(n - 1)


# start = time.time()
#
# print([fib2(n) for n in range(40)])
# end = time.time()
# print('cost:{}'.format(end - start))


@decorate
def climb(n, steps):
    count = 0
    if n <= 1:
        return 1
    else:
        for step in steps:
            print(step, 'step')
            count += climb(n - step, steps)
    return count


print(climb(3, (1, 2)))
