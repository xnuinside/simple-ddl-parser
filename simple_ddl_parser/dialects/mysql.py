from typing import List


class MySQL:
    def p_on_update(self, p: List) -> None:
        """on_update : ON UPDATE id
        | ON UPDATE STRING
        | ON UPDATE f_call
        """
        p_list = list(p)
        if not ")" == p_list[-1]:
            p[0] = {"on_update": p_list[-1]}
        else:
            p[0] = {"on_update": p_list[-2]}
