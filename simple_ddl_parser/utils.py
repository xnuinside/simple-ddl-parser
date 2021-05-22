from typing import List


def remove_par(p_list: List[str]) -> List[str]:
    remove_list = ["(", ")"]
    for symbol in remove_list:
        while symbol in p_list:
            p_list.remove(symbol)
    return p_list


spec_mapper = {
    "'pars_m_t'": "'\t'",
    "'pars_m_n'": "'\n'",
    "'pars_m_dq'": '"',
}


def check_spec(value: str) -> str:
    return spec_mapper.get(value, value)


def find_symbols_not_in_str(str_1: str, str_2: str) -> str:
    # method for development to fast search that symbols are not in parser
    str_1 = set(str_1)
    str_2 = set(str_2)
    not_in_str_2 = str_1 - str_2
    print(f"Symbols not in str 2: {not_in_str_2}")
    return not_in_str_2


def find_first_unpair_closed_par(str_: str) -> int:
    stack = []
    n = -1
    for i in str_:
        n += 1
        if i == ")":
            if not stack:
                return n
            else:
                stack.pop(-1)
        elif i == "(":
            stack.append(i)
