class HQL:
    
    def p_expression_location(self, p):
            """ expr : expr LOCATION STRING
            """
            p[0] = p[1]
            p_list = list(p)
            p[0]['location'] = p_list[-1]
        
    def p_expression_stored_as(self, p):
        """ expr : expr STORED AS ID
        """
        p[0] = p[1]
        p_list = list(p)
        p[0]['stored_as'] = p_list[-1]

    def p_expression_partitioned_by_hql(self, p):
        """ expr : expr PARTITIONED BY LP pid_with_type RP
        """
        p[0] = p[1]
        p_list = list(p)
        p[0]['partitioned_by'] = p_list[-2]