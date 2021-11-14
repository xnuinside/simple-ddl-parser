import simple_ddl_parser  # noqa: F401 weird issue with failed tests


class MySQL:
    def p_on_update(self, p):
        """on_update : ON UPDATE id
        | ON UPDATE STRING
        | ON UPDATE f_call
        """
        p_list = list(p)
        if not ")" == p_list[-1]:
            p[0] = {"on_update": p_list[-1]}
        else:
            p[0] = {"on_update": p_list[-2]}
