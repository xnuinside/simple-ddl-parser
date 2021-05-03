from typing import List


def remove_par(p_list: List[str]) -> List[str]:
    remove_list = ["(", ")"]
    for symbol in remove_list:
        while symbol in p_list:
            p_list.remove(symbol)
    return p_list
