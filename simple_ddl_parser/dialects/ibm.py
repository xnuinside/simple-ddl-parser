from typing import List


class IBMDb2:
    def p_expr_index_in(self, p: List) -> None:
        """expr : expr INDEX IN id"""
        p_list = list(p)
        p[1].update({"index_in": p_list[-1]})
        p[0] = p[1]
