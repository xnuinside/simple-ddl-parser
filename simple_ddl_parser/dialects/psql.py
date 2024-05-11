from typing import List

from simple_ddl_parser.utils import remove_par


class PSQL:
    def p_expr_inherits(self, p: List) -> None:
        """expr : expr INHERITS LP t_name RP"""
        p_list = remove_par(list(p))
        p[0] = p[1]
        table_identifier = {
            "schema": p_list[-1]["schema"],
            "table_name": p_list[-1]["table_name"],
        }
        p[1].update({"inherits": table_identifier})

    def p_timezone(self, p: List) -> None:
        """timezone : WITH id id
        | WITHOUT id id"""
        p_list = remove_par(list(p))
        if "WITH" in p_list:
            timezone = True
        else:
            timezone = False
        p[0] = {"with_time_zone": timezone}
