from typing import List


class IBMDb2:
    def p_expr_index_in(self, p: List) -> None:
        """expr : expr INDEX id id"""
        p_list = list(p)
        if p_list[-2].upper() == "IN":
            p[1].update({"index_in": p_list[-1]})
        p[0] = p[1]
