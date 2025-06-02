def remove_duplicates(lst: list) -> list:
    """
    Remove duplicates from a list while preserving the order.

    Args:
        lst (list): The input list from which duplicates need to be removed.

    Returns:
        list: A new list with duplicates removed.
    """
    seen = set()
    return [x for x in lst if not (x in seen or seen.add(x))]
