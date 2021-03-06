from lib.trees import TreeCalculator, ValueNode, OperationNode
from lib import worker_functions


tree_as_dict = {
    'name': 'modem_is_super_offline',
    'operation': worker_functions.modem_is_super_offline,
    'childs': [
        {
            'name': 'antenna_is_tracking',
            'value': {
                'pk1': True,
                'pk2': False
            },
            'childs': []
        },
        {
            'name': 'is_modem_online',
            'value': {
                'pk1': True,
                'pk2': False
            },
            'childs': []
        },
    ]
}


def create_tree_nodes(tree):
    if 'value' in tree:
        return ValueNode(tree['value'], tree['name'])
    elif not tree.get('operation'):
        raise RuntimeError(f'missing operation in tree: {tree}')

    child_nodes = [create_tree_nodes(child) for child in tree['childs']]

    return OperationNode(tree['operation'], child_nodes, tree['name'])


if __name__ == '__main__':
    root_node = create_tree_nodes(tree_as_dict)

    calculator = TreeCalculator(root_node)

    print(calculator.calculate_tree())
