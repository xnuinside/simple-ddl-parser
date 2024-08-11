from typing import List


class Athena:
    def p_escaped_by(self, p: List) -> None:
        """expr : expr ESCAPED BY STRING_BASE"""
        p[0] = p[1]
        p_list = list(p)
        if "\\\\" in p_list[-1]:
            p_list[-1] = "\\"
        p[0]["escaped_by"] = p_list[-1]
