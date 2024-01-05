from typing import List

from simple_ddl_parser.utils import remove_par


class Snowflake:
    def p_clone(self, p):
        """clone : CLONE id"""
        p_list = list(p)
        p[0] = {"clone": {"from": p_list[-1]}}

    def p_expression_cluster_by(self, p):
        """expr : expr CLUSTER BY LP pid RP
        | expr CLUSTER BY pid
        """
        p[0] = p[1]
        p_list = remove_par(list(p))
        p[0]["cluster_by"] = p_list[-1]

    def p_table_property_equals(self, p: List) -> None:
        """table_property_equals : id id id_or_string
        | id id_or_string
        | LP id id id_or_string RP
        | LP id_or_string RP
        """
        p_list = remove_par(list(p))
        p[0] = str(p_list[-1])

    def p_table_property_equals_int(self, p: List) -> None:
        """table_property_equals_int : id id id_or_string
        | id id_or_string
        | LP id id id_or_string RP
        | LP id_or_string RP
        """
        p_list = remove_par(list(p))
        p[0] = int(p_list[-1])

    def p_table_property_equals_bool(self, p: List) -> None:
        """table_property_equals_bool : id id id_or_string
        | id id_or_string
        """
        p_list = remove_par(list(p))

        if p_list[-1].lower() == "true":
            p[0] = True
        else:
            p[0] = False

    def p_expression_data_retention_time_in_days(self, p):
        """expr : expr DATA_RETENTION_TIME_IN_DAYS table_property_equals_int"""
        p[0] = p[1]
        p_list = remove_par(list(p))
        p[0]["data_retention_time_in_days"] = p_list[-1]

    def p_expression_max_data_extension_time_in_days(self, p):
        """expr : expr MAX_DATA_EXTENSION_TIME_IN_DAYS table_property_equals"""
        p[0] = p[1]
        p_list = remove_par(list(p))
        p[0]["max_data_extension_time_in_days"] = p_list[-1]

    def p_expression_change_tracking(self, p):
        """expr : expr CHANGE_TRACKING table_property_equals_bool"""
        p[0] = p[1]
        p_list = remove_par(list(p))
        p[0]["change_tracking"] = p_list[-1]

    def p_table_comment(self, p):
        """expr : expr option_comment"""
        p[0] = p[1]
        if p[2]:
            p[0].update(p[2])

    def p_table_tag(self, p):
        """expr : expr option_with_tag"""
        p[0] = p[1]
        if p[2]:
            p[0].update(p[2])

    def p_option_comment(self, p):
        """option_comment : ID STRING
        | ID DQ_STRING
        | COMMENT ID STRING
        | COMMENT ID DQ_STRING
        """
        p_list = remove_par(list(p))
        if "comment" in p[1].lower():
            p[0] = {"comment": p_list[-1]}

    def p_tag_equals(self, p: List) -> None:
        """tag_equals : id id id_or_string
        | id id_or_string
        | id DOT id id id_or_string
        | id DOT id id_or_string
        | id DOT id DOT id id id_or_string
        | id DOT id DOT id id_or_string
        """
        # in `id id id_or_string`, the second id is an =
        p_list = remove_par(list(p))
        p[0] = ["".join(p_list[1:])]

    def p_multiple_tag_equals(self, p):
        """multiple_tag_equals : tag_equals
        | multiple_tag_equals COMMA tag_equals
        """
        # Handles multiple tags in the same WITH TAG statement
        if len(p) > 2:
            p[1].extend(p[3])
        p[0] = p[1]

    def p_option_order_noorder(self, p):
        """option_order_noorder : ORDER
        | NOORDER
        """
        p_list = remove_par(list(p))
        p[0] = {"increment_order": True if p_list[1] == "ORDER" else False}

    def p_option_with_tag(self, p):
        """option_with_tag : TAG LP id RP
        | TAG LP id DOT id DOT id RP
        | TAG LP multiple_tag_equals RP
        | WITH TAG LP id RP
        | WITH TAG LP multiple_tag_equals RP
        """
        p_list = remove_par(list(p))
        p[0] = {"with_tag": p_list[-1] if len(p_list[-1]) > 1 else p_list[-1][0]}

    def p_option_with_masking_policy(self, p):
        """option_with_masking_policy : MASKING POLICY id DOT id DOT id
        | WITH MASKING POLICY id DOT id DOT id
        """
        p_list = remove_par(list(p))
        p[0] = {"with_masking_policy": f"{p_list[-5]}.{p_list[-3]}.{p_list[-1]}"}

    def p_expression_catalog(self, p):
        """expr : expr CATALOG table_property_equals"""
        p[0] = p[1]
        p_list = remove_par(list(p))
        p[0]["catalog"] = p_list[-1]

    def p_expression_file_format(self, p):
        """expr : expr FILE_FORMAT table_property_equals"""
        p[0] = p[1]
        p_list = remove_par(list(p))
        p[0]["file_format"] = p_list[-1]

    def p_expression_stage_file_format(self, p):
        """expr : expr STAGE_FILE_FORMAT table_property_equals"""
        p[0] = p[1]
        p_list = remove_par(list(p))
        p[0]["stage_file_format"] = p_list[-1]

    def p_expression_table_format(self, p):
        """expr : expr TABLE_FORMAT table_property_equals"""
        p[0] = p[1]
        p_list = remove_par(list(p))
        p[0]["table_format"] = p_list[-1]

    def p_expression_auto_refresh(self, p):
        """expr : expr AUTO_REFRESH table_property_equals_bool"""
        p[0] = p[1]
        p_list = remove_par(list(p))
        p[0]["auto_refresh"] = p_list[-1]

    def p_as_virtual(self, p: List):
        """as_virtual : AS LP id LP id LP pid RP COMMA pid RP RP
        | AS LP id LP pid RP RP
        | AS LP multi_id RP"""
        _as = ""
        # Simple function else Nested function call
        if len(p) == 5:
            _as = p[3]
        else:
            # _as = p[3]+p[4]+p[5]+p[6]+",".join(p[7])+p[8]+p[9]+",".join(p[10])+p[11]
            for i in p[3:len(p) - 1]:
                _as += i if isinstance(i, str) else ",".join(i)
        p[0] = {"generated": {"as": _as}}
