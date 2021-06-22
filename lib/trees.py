from __future__ import annotations

import multiprocessing
import typing
from collections import defaultdict
from functools import partial


class SingletonMeta(type):
    # This is a metaclass that is only aimed to be used with TreeWorkerPool
    _instance = None

    def __call__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(SingletonMeta, cls).__call__(*args, **kwargs)
        return cls._instance


class TreeWorkerPool(metaclass=SingletonMeta):
    # This is a singleton class wrapping a multiprocessing pool with a method to execute a function in a pool worker
    def __init__(self):
        self._pool = multiprocessing.Pool(
            processes=None,
            initializer=None,
            initargs=[],
            maxtasksperchild=None
        )

    def execute_async(
            self,
            callback: typing.Callable,
            operation: typing.Callable,
            kwargs: typing.Dict[str, typing.Union[str, int, float, bool, None]] = None
    ) -> multiprocessing.pool.AsyncResult:
        if not kwargs:
            kwargs = {}

        def raise_exc(e):
            raise RuntimeError(f'An exception occurred in a pool thread: {e}')
        return self._pool.apply_async(operation, args=[], kwds=kwargs, callback=callback, error_callback=raise_exc)
        
    def execute_and_wait(
            self,
            operation: typing.Callable,
            args: typing.List = None
    ):
        if not args:
            args = []
        return self._pool.apply(operation, args=args)


class Node:
    def __init__(
            self,
            node_name: str,
            operation: typing.Callable = None,
            value: typing.Dict[str, typing.Union[str, int, float, bool, None]] = None,
            childs: typing.List[Node] = None,
            parent: typing.Optional[Node] = None
    ):
        if not childs:
            childs = []
        if operation and value:
            raise RuntimeError("Either set a value or an operation, but not both")
        if not value:
            value = {}
        self.value: typing.Dict[str, typing.Union[str, int, float, bool, None]] = value
        self.operation: typing.Optional[typing.Callable] = operation
        self.childs: typing.Optional[typing.List[Node]] = childs
        self.parent: typing.Optional[Node] = parent
        for child in self.childs:
            child.parent = self
        self.child_results_by_pk: typing.Dict[
            str, typing.Dict[str, typing.Union[str, int, float, bool, None]]
        ] = defaultdict(dict)
        self.node_name = node_name


class ValueNode(Node):
    def __init__(
            self,
            value: typing.Dict[str, typing.Union[str, int, float, bool, None]],
            node_name: str
    ):
        super(ValueNode, self).__init__(
            value=value,
            node_name=node_name
        )

    def __repr__(self):
        return f'Value node({self.value})'


class OperationNode(Node):
    def __init__(
            self,
            operation: typing.Callable,
            childs: typing.List[Node],
            node_name: str
    ):
        super(OperationNode, self).__init__(
            operation=operation,
            childs=childs,
            node_name=node_name
        )

    def __repr__(self):
        return f'Operation Node {self.operation.__name__}' \
               f'({", ".join([repr(child) for child in self.childs])})'


class TreeCalculator:
    def __init__(
            self,
            root_node: Node
    ):
        self._root_node: Node = root_node
        self._nodes_by_level: typing.Dict[int, typing.List[Node]] = {}
        self._calculate_tree_levels()

    def _calculate_tree_levels(self):
        self._nodes_by_level[0] = [self._root_node]
        current_level = 0
        while True:
            level_childs = self._get_level_childs(self._nodes_by_level[current_level])
            if level_childs:
                self._nodes_by_level[current_level+1] = level_childs
                current_level += 1
            else:
                break

    @staticmethod
    def _get_level_childs(level: typing.List[Node]):
        return [
            child for node in level for child in node.childs
        ]

    def calculate_tree(self):
        levels: typing.List[typing.List[Node]] = [
            level_nodes
            for level, level_nodes in
            sorted(self._nodes_by_level.items(), reverse=True, key=lambda kv: kv[0])
        ]

        for level in levels:
            worker_results: typing.List[multiprocessing.pool.AsyncResult] = []
            for node in level:
                if type(node) == ValueNode:
                    for pk, value in node.value.items():
                        node.parent.child_results_by_pk[pk][node.node_name] = value
                elif type(node) == OperationNode:
                    for pk, results_by_child_name in node.child_results_by_pk.items():
                        def handler(_node, _pk, _result):
                            _node.value[_pk] = _result
                            if _node.parent:
                                _node.parent.child_results_by_pk[_pk][_node.node_name] = _result

                        worker_results.append(TreeWorkerPool().execute_async(
                            partial(handler, node, pk),
                            node.operation,
                            results_by_child_name
                        ))
            for r in worker_results:
                r.wait()

        return self._root_node.value


def create_tree_nodes(tree):
    if 'value' in tree:
        return ValueNode(tree['value'], tree['name'])
    elif not tree.get('operation'):
        raise RuntimeError(f'missing operation in tree: {tree}')

    child_nodes = [create_tree_nodes(child) for child in tree['childs']]

    return OperationNode(tree['operation'], child_nodes, tree['name'])
