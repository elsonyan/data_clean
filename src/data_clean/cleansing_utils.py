import json
import os, yaml
from typing import Any, List
from pyspark.sql.functions import col


class Origin_Rule(object):
    def __init__(self, *args):
        for arg in args:
            for k, v in arg.items():
                if isinstance(v, dict):
                    self.__dict__[k] = Origin_Rule(v)
                else:
                    self.__dict__[k] = v

    # def __getattribute__(self, name):
    #     # use object.__getattribute__ to get the private attr
    #     try:
    #         return object.__getattribute__(self, name.lower())
    #     except Exception as e:
    #         print(e)
    #         return None

    def __str__(self) -> str:
        return json.dumps(self, default=lambda o: o.__dict__, indent=4)


# load yaml config as a nested class
def load_yaml_rules(yaml_path: str) -> Origin_Rule:
    if os.path.exists(yaml_path):
        with open(yaml_path, mode="r", encoding="UTF-8") as file:
            env_objs = yaml.load(file, Loader=yaml.FullLoader)
        return Origin_Rule(env_objs)
    else:
        raise FileNotFoundError(f"{yaml_path} not found")


def load_cols(cols: tuple) -> [col]:
    return [col(c) for c in cols]


def entire_exist(driver: list, attach: list) -> bool:
    intersect = set(driver) & set([att_col.lower() for att_col in attach])
    return True if len(intersect) == len(set(attach)) else False


def intersection(A: list, B: list) -> list:
    return list(set([a.lower() for a in A]).intersection(set([b.lower() for b in B])))


# Node for Queue
class Node(object):
    def __init__(self,
                 data: Any = None,
                 prev=None,
                 next=None, ):
        self.data: Any = data
        self.prev: Node = prev
        self.next: Node = next


class Queue:
    head: Node = None
    tail: Node = None

    @property
    def shift(self):
        if not self.head:
            return None
        item = self.head.data
        self.head = self.head.next
        if not self.head:
            self.tail = None
        else:
            self.head.prev = None
        return item

    @property
    def pop(self):
        if not self.tail:
            return None
        item = self.tail.data
        if not self.tail.prev:
            self.head = None
        else:
            self.tail = self.tail.prev
            self.tail.next = None
        return item

    @property
    def size(self) -> int:
        size = 0
        current = self.head
        while True:
            if not current:
                break
            size += 1
            current = current.next
        return size

    def append(self, data):
        node = Node(data)
        if not self.head:
            self.head = node
        else:
            curr = self.head
            while curr.next:
                curr = curr.next
            curr.next = node
            self.tail = node
            self.tail.prev = curr

    def append_list(self, data_list: list):
        for data in data_list:
            node = Node(data)
            if not self.head:
                self.head = node
            else:
                curr = self.head
                while curr.next:
                    curr = curr.next
                curr.next = node

    def append_tuple(self, data_list: tuple):
        for data in data_list:
            node = Node(data)
            if not self.head:
                self.head = node
            else:
                curr = self.head
                while curr.next:
                    curr = curr.next
                curr.next = node

    @staticmethod
    def showNode(node: Node):
        print(node, node.__dict__)

    def list(self):
        current = self.head
        while True:
            if not current:
                break
            self.showNode(current)
            current = current.next
