from simple_ddl_parser.utils import remove_par


class Oracle:
    def p_encrypt(self, p):
        """encrypt : ENCRYPT
        | encrypt NO SALT
        | encrypt SALT
        | encrypt USING STRING
        | encrypt STRING
        """
        p_list = list(p)
        if isinstance(p[1], dict):
            p[0] = p[1]
            if "NO" in p_list:
                p[0]["encrypt"]["salt"] = False
            elif "USING" in p_list:
                p[0]["encrypt"]["encryption_algorithm"] = p_list[-1]
            elif "SALT" not in p_list:
                p[0]["encrypt"]["integrity_algorithm"] = p_list[-1]

        else:
            p[0] = {
                "encrypt": {
                    "salt": True,
                    "encryption_algorithm": "'AES192'",
                    "integrity_algorithm": "SHA-1",
                }
            }

    def p_storage(self, p):
        """storage : STORAGE LP
        | storage ID ID
        | storage ID ID RP
        """
        # Initial 5m Next 5m Maxextents Unlimited
        p_list = remove_par(list(p))
        param = {}
        if len(p_list) == 4:
            param = {p_list[2].lower(): p_list[3]}
        if isinstance(p_list[1], dict):
            p[0] = p[1]
        else:
            p[0] = {}
        p[0].update(param)

    def p_expr_storage(self, p):
        """expr : expr storage"""
        p_list = list(p)
        p[0] = p[1]
        p[0]["storage"] = p_list[-1]
