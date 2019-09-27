# -*- coding:utf-8 -*-
"""
@author:zzw
@file: 合并两个有序链表.py
@time: 2019/09/25
"""
"""
数据结构 - 链表介绍：
  - 大致分为单链表和双向链表
      - 单链表：每个节点的包含两部分 （数据，和指向下个节点next指针）
      - 双向链表：除了包含单个链表部分，还添加pre前一个节点指针
  - 优点：不需要连续存储单元，修改链表复杂度O(1) (在不考虑查找)
  - 缺点：无法找到的指点的节点，只能从头一步步查找,复杂度是O(n)
"""

"""
解题思路，用的迭代方式实现
     - 每次判断两个节点的头部数据比较大小
"""


class ListNode(object):
    pass


class Solution:
    def mergeSortedArray_1(self, A, B):
        """
        :type l1: ListNode
        :type l2: ListNode
        :rtype: ListNode
        """
        # herad = []
        # # 先定义初始位置
        # i = 0
        # j = 0
        # acount = len(A)
        # bcount = len(B)
        # while i < acount or j < bcount:
        #     # 先如果的A都出完了
        #     if i == acount:
        #         herad.append(B[j])
        #         j += 1
        #     elif j == bcount:
        #         herad.append(A[i])
        #         i += 1
        #     elif A[i] < B[j]:
        #         herad.append(A[i])
        #         i += 1
        #     else:
        #         herad.append(B[j])
        #         j += 1
        # return herad
        herad = []
        while A or B:
            if not A or not B:
                if A:
                    herad.append(A[0])
                    A.remove(A[0])
                else:
                    herad.append(B[0])
                    B.remove(B[0])
            else:
                if A[0] < B[0]:
                    herad.append(A[0])
                    A.remove(A[0])
                else:
                    herad.append(B[0])
                    B.remove(B[0])
        return herad
if __name__ == "__main__":
    A = [1, 2, 3, 4]
    B = [2, 4, 5]
    print(A[1])
    # 使用迭代方式实现代码
    herad = Solution().mergeSortedArray_1(A, B)
    print(herad, "==")
