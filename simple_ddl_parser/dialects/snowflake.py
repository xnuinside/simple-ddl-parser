from typing import List, Union

from simple_ddl_parser.utils import remove_par


# todo: move to utils module
def convert_to_python_bool(value: str) -> Union[bool, str]:
    value = value.lower().strip()
    if value == "true":
        return True
    elif value == "false":
        return False
    return value


def convert_to_python_int(value: str) -> Union[int, str]:
    try:
        return int(value)
    except Exception:
        return value


class Snowflake:
    def p_clone(self, p: List) -> None:
        """clone : CLONE id"""
        p_list = list(p)
        p[0] = {"clone": {"from": p_list[-1]}}

    def p_expression_cluster_by(self, p: List) -> None:
        """expr : expr cluster_by"""
        p_list = list(p)
        p[0] = p[1]
        p[0].update(p_list[-1])

    def p_cluster_by(self, p: List) -> None:
        """cluster_by : CLUSTER BY LP pid RP
        | CLUSTER BY pid
        """
        p_list = remove_par(list(p))
        p[0] = {"cluster_by": p_list[-1]}

    def p_multi_id_or_string(self, p: List) -> None:
        """multi_id_or_string : id_or_string
        | EQ id_or_string
        | id DOT multi_id_or_string
        | multi_id_or_string EQ id_or_string"""
        p_list = list(p)
        if isinstance(p[1], list):
            p[0] = p[1]
            p[0].append(p_list[-1])
        else:
            totrim = " ".join(p_list[1:])
            p[0] = totrim.replace(" = ", "=").replace("= ", "").replace(" . ", ".")

    # todo: need to review & maybe simplify / remove
    def p_table_property_equals(self, p: List) -> None:
        """table_property_equals : id EQ id_or_string
        | EQ id_or_string
        | id id_or_string
        | id DOT id_or_string
        | id DOT id DOT id_or_string
        | LP id id id_or_string RP
        | LP id_or_string RP
        | id table_property_equals
        | id_equals
        | multi_id_equals
        """
        p_list = remove_par(list(p))
        p[0] = str(p_list[-1])

    def p_expression_data_retention_time_in_days(self, p: List) -> None:
        """expr : expr DATA_RETENTION_TIME_IN_DAYS EQ ID"""
        p[0] = p[1]
        p_list = remove_par(list(p))
        p[0]["data_retention_time_in_days"] = convert_to_python_int(p_list[-1])

    def p_expression_max_data_extension_time_in_days(self, p: List) -> None:
        """expr : expr MAX_DATA_EXTENSION_TIME_IN_DAYS table_property_equals"""
        p[0] = p[1]
        p_list = remove_par(list(p))
        p[0]["max_data_extension_time_in_days"] = p_list[-1]

    def p_expression_change_tracking(self, p: List) -> None:
        """expr : expr CHANGE_TRACKING EQ ID"""
        p[0] = p[1]
        p_list = remove_par(list(p))

        p[0]["change_tracking"] = convert_to_python_bool(p_list[-1])

    def p_comment_equals(self, p: List) -> None:
        """expr : expr option_comment"""
        p[0] = p[1]
        if p[2]:
            p[0].update(p[2])

    def p_option_comment(self, p: List) -> None:
        """option_comment : EQ STRING
        | EQ DQ_STRING
        | COMMENT EQ STRING
        | COMMENT EQ DQ_STRING
        | option_comment_equals
        """
        p_list = remove_par(list(p))
        p[0] = {"comment": p_list[-1]}

    def p_option_comment_equals(self, p: List) -> None:
        """option_comment_equals : STRING
        | option_comment_equals DQ_STRING
        """
        p_list = remove_par(list(p))
        p[0] = str(p_list[-1])

    def p_tag(self, p: List) -> None:
        """expr : expr option_with_tag"""
        p[0] = p[1]
        if p[2]:
            p[0].update(p[2])

    def p_tag_equals(self, p: List) -> None:
        """tag_equals : id EQ id_or_string
        | id id_or_string
        | dot_id EQ id_or_string
        | dot_id id_or_string
        """
        # in `id id id_or_string`, the second id is an =
        p_list = remove_par(list(p))
        p[0] = ["".join(p_list[1:])]

    def p_multiple_tag_equals(self, p: List) -> None:
        """multiple_tag_equals : tag_equals
        | multiple_tag_equals COMMA tag_equals
        """
        # Handles multiple tags in the same WITH TAG statement
        if len(p) > 2:
            p[1].extend(p[3])
        p[0] = p[1]

    def p_option_order_noorder(self, p: List) -> None:
        """option_order_noorder : ORDER
        | NOORDER
        """
        p_list = remove_par(list(p))
        p[0] = {"increment_order": True if p_list[1] == "ORDER" else False}

    def p_option_with_tag(self, p: List) -> None:
        """option_with_tag : TAG LP id RP
        | TAG LP dot_id DOT id RP
        | TAG LP multiple_tag_equals RP
        | WITH TAG LP id RP
        | WITH TAG LP dot_id DOT id RP
        | WITH TAG LP multiple_tag_equals RP
        """
        p_list = remove_par(list(p))
        p[0] = {"with_tag": p_list[-1] if len(p_list[-1]) > 1 else p_list[-1][0]}

    def p_option_with_masking_policy(self, p: List) -> None:
        """option_with_masking_policy : MASKING POLICY id DOT id DOT id
        | WITH MASKING POLICY id DOT id DOT id
        """
        p_list = remove_par(list(p))
        p[0] = {"with_masking_policy": f"{p_list[-5]}.{p_list[-3]}.{p_list[-1]}"}

    def p_expression_catalog(self, p: List) -> None:
        """expr : expr CATALOG table_property_equals"""
        p[0] = p[1]
        p_list = remove_par(list(p))
        p[0]["catalog"] = p_list[-1]

    def p_expression_file_format(self, p: List) -> None:
        """expr : expr FILE_FORMAT EQ LP multi_id_equals RP
        | expr FILE_FORMAT EQ ID
        """
        p[0] = p[1]
        p_list = remove_par(list(p))
        p[0]["file_format"] = p_list[-1]

    def p_expression_stage_file_format(self, p: List) -> None:
        """expr : expr STAGE_FILE_FORMAT EQ LP multi_id_equals RP
        | expr STAGE_FILE_FORMAT EQ ID"""
        p[0] = p[1]
        p_list = remove_par(list(p))
        p[0]["stage_file_format"] = p_list[-1] if len(p_list[-1]) > 1 else p_list[-1][0]

    def p_expression_table_format(self, p: List) -> None:
        """expr : expr TABLE_FORMAT table_property_equals"""
        p[0] = p[1]
        p_list = remove_par(list(p))
        p[0]["table_format"] = p_list[-1]

    def p_expression_auto_refresh(self, p: List) -> None:
        """expr : expr AUTO_REFRESH EQ ID"""
        p[0] = p[1]
        p_list = remove_par(list(p))

        p[0]["auto_refresh"] = convert_to_python_bool(p_list[-1])

    def p_expression_pattern(self, p: List) -> None:
        """expr : expr PATTERN table_property_equals"""
        p[0] = p[1]
        p_list = remove_par(list(p))
        p[0]["pattern"] = p_list[-1]

    def p_recursive_pid(self, p: List) -> None:
        """recursive_pid : pid
        | multi_id
        | id LP RP
        | id LP pid RP
        | id LP pid RP pid
        | id COMMA pid
        | id LP id LP recursive_pid RP COMMA pid RP
        | multi_id LP pid RP
        | id LP multi_id RP
        | id LP id AS recursive_pid RP
        | id LP id LP recursive_pid RP AS recursive_pid RP
        """
        p_list = list(p)
        items = [item if isinstance(item, str) else ",".join(item) for item in p_list[1:]]
        p[0] = "".join(items)

    def p_as_virtual(self, p: List):
        """as_virtual : AS LP id RP
        | AS LP recursive_pid RP
        | AS LP id LP id LP multi_id COMMA pid RP AS recursive_pid RP RP"""
        _as = ""
        # Simple function else Nested function call
        if len(p) == 5:
            _as = p[3]
        else:
            for i in p[3 : len(p) - 1]:  # noqa: E203
                _as += i if isinstance(i, str) else ",".join(i)
        p[0] = {"generated": {"as": _as}}
