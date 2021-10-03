from simple_ddl_parser.utils import check_spec, remove_par


class MSSQL:
    def p_pkey_constraint(self, p):
        """pkey_constraint : constraint pkey_statement ID LP index_pid RP
        | constraint pkey_statement LP index_pid RP
        """
        p_list = list(p)
        p[0] = p[1]
        print(p[0])
        if len(p_list)  == 7:
            data = {"primary_key": {"columns": p_list[-2]}, p[3]: True}
        else:
            data = {"primary_key": {"columns": p_list[-2]}}
        p[0]['constraint'].update(data)

