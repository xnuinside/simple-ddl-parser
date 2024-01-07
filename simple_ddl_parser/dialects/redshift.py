from typing import List


class Redshift:
    def p_expression_distkey(self, p: List) -> None:
        """expr : expr id LP id RP"""
        p_list = list(p)
        p[1].update({"distkey": p_list[-2]})
        p[0] = p[1]

    def p_encode(self, p: List) -> None:
        """encode : ENCODE id"""
        p_list = list(p)
        p[0] = {"encode": p_list[-1]}

    def p_expression_diststyle(self, p: List) -> None:
        """expr : expr id id
        | expr id KEY
        """
        p_list = list(p)
        if p_list[-2] == "IN":
            # mean we in 'IN TABLESPACE'
            p[1].update({"tablespace": p_list[-1]})
        else:
            p[1].update({p_list[-2]: p_list[-1]})
        p[0] = p[1]

    def p_expression_sortkey(self, p: List) -> None:
        """expr : expr id id LP pid RP"""
        p_list = list(p)
        p[1].update({"sortkey": {"type": p_list[2], "keys": p_list[-2]}})
        p[0] = p[1]
