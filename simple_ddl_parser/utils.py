import re
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
    "pars_m_single": "'",
}


def check_spec(value: str) -> str:
    replace_value = spec_mapper.get(value)
    if not replace_value:
        for item in spec_mapper:
            if item in value:
                replace_value = value.replace(item, spec_mapper[item])
                break
        else:
            replace_value = value
    return replace_value


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


def normalize_name(name: str) -> str:
    # clean up [] and " symbols from names
    clean_up_re = r'[\[\]"]'
    return re.sub(clean_up_re, "", name).lower()


def get_table_id(schema_name: str, table_name: str):
    table_name = normalize_name(table_name)
    if schema_name:
        schema_name = normalize_name(schema_name)
    return (table_name, schema_name)


class SimpleDDLParserException(Exception):
    pass
