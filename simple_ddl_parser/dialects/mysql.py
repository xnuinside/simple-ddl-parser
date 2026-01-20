from typing import List


class MySQL:
    def p_engine(self, p: List) -> None:
        """expr : expr ENGINE EQ id"""
        p_list = list(p)
        p[0] = p[1]
        p[0]["engine"] = p_list[-1]

    def p_db_properties(self, p: List) -> None:
        """expr : expr id EQ id_or_string"""
        p_list = list(p)
        p[0] = p[1]
        p[0][p[2]] = p_list[-1]

    def p_character_set_table_option(self, p: List) -> None:
        """expr : expr id SET EQ id
        | expr id SET id"""
        # Handles table-level CHARACTER SET = value and CHARACTER SET value syntax
        p_list = list(p)
        p[0] = p[1]
        if p_list[2].upper() == "CHARACTER":
            # Use "character" key for backward compatibility
            p[0]["character"] = p_list[-1]
        else:
            # Fallback for other id SET = value patterns
            p[0][f"{p_list[2].lower()}_{p_list[3].lower()}"] = p_list[-1]

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
