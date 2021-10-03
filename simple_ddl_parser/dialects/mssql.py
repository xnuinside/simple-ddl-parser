from simple_ddl_parser.utils import check_spec, remove_par


class MSSQL:
    def p_pkey_constraint(self, p):
        """pkey_constraint : constraint pkey_statement ID LP index_pid RP
        | constraint pkey_statement LP index_pid RP
        | pkey_constraint with
        """
        p_list = list(p)
        print(p_list, 'pkey_constraint')
        p[0] = p[1]
        if len(p_list) == 3:
            data = p_list[-1]
            print(data)
        elif len(p_list)  == 7:
            data = {"primary_key": True,
                    "columns": p_list[-2],
                    p[3]: True}
        else:
            data = {"primary_key": True,
                    "columns": p_list[-2]}
        p[0]['constraint'].update(data)

    def p_with(self, p):
        """with : WITH LP ID ID ID
        | with COMMA ID ID ID
        | WITH LP ID ID ON
        | with COMMA ID ID ON
        | with RP
        | with RP ON ID
        """
        p_list = list(p)
        print(p_list, 'with')
        if isinstance(p[1], dict):
            p[0] = p[1]
        else:
            p[0] = {'with': {
                'properties': [],
            'on': None}
            }
        if not ')' in p_list:
            p[0]['with']['properties'].append({'name': p_list[-3], 'value': p_list[-1]})
        elif 'ON' in p_list:
            p[0]['with']['on'] = p_list[-1]

