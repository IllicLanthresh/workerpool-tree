import time

from lib.trees import TreeCalculator, create_tree_nodes
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

if __name__ == '__main__':
    root_node = create_tree_nodes(tree_as_dict)

    calculator = TreeCalculator(root_node)

    calculator.calculate_tree(print)

    while True:
        time.sleep(0.1)
