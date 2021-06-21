from lib.trees import TreeCalculator, create_tree_nodes
from lib import worker_functions


tree_as_dict = {
    'operation': worker_functions.my_sum,
    'childs': [
        {
            'operation': worker_functions.fib_term,
            'childs': [
                {
                    'value': 10000,
                    'childs': []
                }
            ]
        },
        {
            'operation': worker_functions.fib_term,
            'childs': [
                {
                    'value': 500,
                    'childs': []
                }
            ]
        },
    ]
}

if __name__ == '__main__':
    root_node = create_tree_nodes(tree_as_dict)

    calculator = TreeCalculator(root_node)

    print(calculator.calculate_tree())
