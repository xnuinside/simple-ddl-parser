from simple_ddl_parser.utils import check_spec, remove_par


class MySQL:
    def p_on_update(self, p):
        """on_update : ON UPDATE ID
        | ON UPDATE STRING
        | ON UPDATE f_call
        """
        p_list = list(p)
        if not ')' ==  p_list[-1]:
            p[0] = {"on_update": p_list[-1]}
        else:
            p[0] = {"on_update": p_list[-2]}

