# -*- coding:utf-8 -*-
"""
@author:zzw
@file: 逆波兰表达式求值.py
@time: 2019/09/24
"""
"""
输入: ["2", "1", "+", "3", "*"] 
输出: 9
解释: ["2", "1", "+", "3", "*"] -> (2 + 1) * 3 -> 9

解题思路
  - 入到一个字符串就进行 前面两个进行计算
  - 注意： 当 r//l 不能被整除
    - 先判断 result 小于0在基础上+1
  result = result + 1 if result < 0 and r % l != 0 else result

根据这样的规则，用到的（栈）
"""


class Solution:
    """
    @param tokens: The Reverse Polish Notation
    @return: the value
    """

    def evalRPN(self, tokens):
        # write your code here
        # 第一步，我们先循环字符串
        stack = []
        for val in tokens:
            if val == "+":
                stack.append(stack.pop() + stack.pop())
            elif val == "-":
                stack.append(-stack.pop() + stack.pop())
            elif val == "*":
                stack.append(stack.pop() * stack.pop())
            elif val == "/":
                l = stack.pop()
                r = stack.pop()
                result = r // l
                result = result + 1 if result < 0 and r % l != 0 else result
                stack.append(round(result))
            else:
                stack.append(int(val))
            print(stack, "stack")
        return int(stack.pop())


if __name__ == '__main__':
    data = ["4", "13", "5", "/", "+"]

    ac = Solution().evalRPN(data)
    print(ac)
