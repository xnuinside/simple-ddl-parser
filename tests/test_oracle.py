from simple_ddl_parser import DDLParser


def test_encrypt():

    ddl = """

CREATE TABLE employee (
     first_name VARCHAR2(128),
     last_name VARCHAR2(128),
     salary_1 NUMBER(6) ENCRYPT,
     empID NUMBER ENCRYPT NO SALT,
     salary NUMBER(6) ENCRYPT USING '3DES168');

CREATE TABLE employee_2 (
     first_name VARCHAR2(128),
     last_name VARCHAR2(128),
     empID NUMBER ENCRYPT 'NOMAC' ,
     salary NUMBER(6));

"""
    result = DDLParser(ddl).run(group_by_type=True)
    expected = {
        "sequences": [],
        "tables": [
            {
                "alter": {},
                "checks": [],
                "columns": [
                    {
                        "check": None,
                        "default": None,
                        "name": "first_name",
                        "nullable": True,
                        "references": None,
                        "size": 128,
                        "type": "VARCHAR2",
                        "unique": False,
                    },
                    {
                        "check": None,
                        "default": None,
                        "name": "last_name",
                        "nullable": True,
                        "references": None,
                        "size": 128,
                        "type": "VARCHAR2",
                        "unique": False,
                    },
                    {
                        "check": None,
                        "default": None,
                        "encrypt": {
                            "encryption_algorithm": "'AES192'",
                            "integrity_algorithm": "SHA-1",
                            "salt": True,
                        },
                        "name": "salary_1",
                        "nullable": True,
                        "references": None,
                        "size": 6,
                        "type": "NUMBER",
                        "unique": False,
                    },
                    {
                        "check": None,
                        "default": None,
                        "encrypt": {
                            "encryption_algorithm": "'AES192'",
                            "integrity_algorithm": "SHA-1",
                            "salt": False,
                        },
                        "name": "empID",
                        "nullable": True,
                        "references": None,
                        "size": None,
                        "type": "NUMBER",
                        "unique": False,
                    },
                    {
                        "check": None,
                        "default": None,
                        "encrypt": {
                            "encryption_algorithm": "'3DES168'",
                            "integrity_algorithm": "SHA-1",
                            "salt": True,
                        },
                        "name": "salary",
                        "nullable": True,
                        "references": None,
                        "size": 6,
                        "type": "NUMBER",
                        "unique": False,
                    },
                ],
                "index": [],
                "partitioned_by": [],
                "primary_key": [],
                "schema": None,
                "table_name": "employee",
            },
            {
                "alter": {},
                "checks": [],
                "columns": [
                    {
                        "check": None,
                        "default": None,
                        "name": "first_name",
                        "nullable": True,
                        "references": None,
                        "size": 128,
                        "type": "VARCHAR2",
                        "unique": False,
                    },
                    {
                        "check": None,
                        "default": None,
                        "name": "last_name",
                        "nullable": True,
                        "references": None,
                        "size": 128,
                        "type": "VARCHAR2",
                        "unique": False,
                    },
                    {
                        "check": None,
                        "default": None,
                        "encrypt": {
                            "encryption_algorithm": "'AES192'",
                            "integrity_algorithm": "'NOMAC'",
                            "salt": True,
                        },
                        "name": "empID",
                        "nullable": True,
                        "references": None,
                        "size": None,
                        "type": "NUMBER",
                        "unique": False,
                    },
                    {
                        "check": None,
                        "default": None,
                        "name": "salary",
                        "nullable": True,
                        "references": None,
                        "size": 6,
                        "type": "NUMBER",
                        "unique": False,
                    },
                ],
                "index": [],
                "partitioned_by": [],
                "primary_key": [],
                "schema": None,
                "table_name": "employee_2",
            },
        ],
        "types": [],
    }
    assert expected == result
