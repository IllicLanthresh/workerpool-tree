def fib_term(index):
    # This is just an intentionally slow way of getting the nth fibonacci sequence term
    n1, n2 = 0, 1

    for _ in range(index):
        n1, n2 = n2, n1 + n2
    return n1


def my_sum(*args):
    return sum(args)
