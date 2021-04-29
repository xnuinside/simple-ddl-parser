class Oracle:
    def p_encrypt(self, p):
        """encrypt : ENCRYPT
        | encrypt NO SALT
        | encrypt SALT
        | encrypt USING STRING
        | encrypt STRING
        """
        p_list = list(p)
        print(p_list)
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
