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
        "domains": [],
        "schemas": [],
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
                "tablespace": None,
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
                "tablespace": None,
            },
        ],
        "types": [],
        "ddl_properties": [],
    }
    assert expected == result


def test_oracle_output_mode():
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

    result = DDLParser(ddl).run(group_by_type=True, output_mode="oracle")
    expected = {
        "sequences": [],
        "domains": [],
        "schemas": [],
        "tables": [
            {
                "alter": {},
                "checks": [],
                "columns": [
                    {
                        "check": None,
                        "default": None,
                        "encrypt": None,
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
                        "encrypt": None,
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
                        "encrypt": None,
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
                        "encrypt": None,
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
                        "encrypt": None,
                        "name": "salary",
                        "nullable": True,
                        "references": None,
                        "size": 6,
                        "type": "NUMBER",
                        "unique": False,
                    },
                ],
                "constraints": {"checks": None, "references": None, "uniques": None},
                "index": [],
                "partitioned_by": [],
                "primary_key": [],
                "schema": None,
                "storage": None,
                "table_name": "employee",
                "tablespace": None,
            },
            {
                "alter": {},
                "checks": [],
                "columns": [
                    {
                        "check": None,
                        "default": None,
                        "encrypt": None,
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
                        "encrypt": None,
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
                        "encrypt": None,
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
                        "encrypt": None,
                        "name": "salary",
                        "nullable": True,
                        "references": None,
                        "size": 6,
                        "type": "NUMBER",
                        "unique": False,
                    },
                ],
                "constraints": {"checks": None, "references": None, "uniques": None},
                "index": [],
                "partitioned_by": [],
                "primary_key": [],
                "schema": None,
                "storage": None,
                "table_name": "employee_2",
                "tablespace": None,
            },
        ],
        "types": [],
        "ddl_properties": [],
    }
    assert expected == result


def test_storage():
    expected = {
        "sequences": [],
        "domains": [],
        "schemas": [],
        "tables": [
            {
                "alter": {},
                "checks": [],
                "columns": [
                    {
                        "check": None,
                        "default": None,
                        "encrypt": None,
                        "name": "empno",
                        "nullable": True,
                        "references": None,
                        "size": None,
                        "type": "Number",
                        "unique": False,
                    },
                    {
                        "check": None,
                        "default": None,
                        "encrypt": None,
                        "name": "ename",
                        "nullable": True,
                        "references": None,
                        "size": 100,
                        "type": "Varchar2",
                        "unique": False,
                    },
                    {
                        "check": None,
                        "default": None,
                        "encrypt": None,
                        "name": "sal",
                        "nullable": True,
                        "references": None,
                        "size": None,
                        "type": "Number",
                        "unique": False,
                    },
                    {
                        "check": None,
                        "default": None,
                        "encrypt": None,
                        "name": "photo",
                        "nullable": True,
                        "references": None,
                        "size": None,
                        "type": "Blob",
                        "unique": False,
                    },
                ],
                "constraints": {"checks": None, "references": None, "uniques": None},
                "index": [],
                "partitioned_by": [],
                "primary_key": [],
                "schema": None,
                "storage": {"initial": "5m", "maxextents": "Unlimited", "next": "5m"},
                "table_name": "emp_table",
                "tablespace": None,
            }
        ],
        "types": [],
        "ddl_properties": [],
    }
    ddl = """

Create Table emp_table (
empno Number,
ename Varchar2(100),
sal Number,
photo Blob
)
Storage ( Initial 5m Next 5m Maxextents Unlimited )
"""

    result = DDLParser(ddl).run(group_by_type=True, output_mode="oracle")
    assert expected == result


def test_partition_by():
    ddl = """
CREATE TABLE order_items
    ( order_id           NUMBER(12) NOT NULL,
      line_item_id       NUMBER(3)  NOT NULL,
      product_id         NUMBER(6)  NOT NULL,
      unit_price         NUMBER(8,2),
      quantity           NUMBER(8),
      CONSTRAINT order_items_fk
      FOREIGN KEY(order_id) REFERENCES orders(order_id)
    )
    PARTITION BY REFERENCE(order_items_fk);
"""
    result = DDLParser(ddl).run(group_by_type=True)
    expected = {
        "domains": [],
        "ddl_properties": [],
        "schemas": [],
        "sequences": [],
        "tables": [
            {
                "alter": {},
                "checks": [],
                "columns": [
                    {
                        "check": None,
                        "default": None,
                        "name": "order_id",
                        "nullable": False,
                        "references": None,
                        "size": 12,
                        "type": "NUMBER",
                        "unique": False,
                    },
                    {
                        "check": None,
                        "default": None,
                        "name": "line_item_id",
                        "nullable": False,
                        "references": None,
                        "size": 3,
                        "type": "NUMBER",
                        "unique": False,
                    },
                    {
                        "check": None,
                        "default": None,
                        "name": "product_id",
                        "nullable": False,
                        "references": None,
                        "size": 6,
                        "type": "NUMBER",
                        "unique": False,
                    },
                    {
                        "check": None,
                        "default": None,
                        "name": "unit_price",
                        "nullable": True,
                        "references": None,
                        "size": (8, 2),
                        "type": "NUMBER",
                        "unique": False,
                    },
                    {
                        "check": None,
                        "default": None,
                        "name": "quantity",
                        "nullable": True,
                        "references": None,
                        "size": 8,
                        "type": "NUMBER",
                        "unique": False,
                    },
                ],
                "constraints": {
                    "references": [
                        {
                            "columns": ["order_id"],
                            "constraint_name": "order_items_fk",
                            "deferrable_initially": None,
                            "on_delete": None,
                            "on_update": None,
                            "schema": None,
                            "table": "orders",
                        }
                    ]
                },
                "index": [],
                "partition_by": {"columns": ["order_items_fk"], "type": "REFERENCE"},
                "partitioned_by": [],
                "primary_key": [],
                "schema": None,
                "table_name": "order_items",
                "tablespace": None,
            }
        ],
        "types": [],
    }
    assert expected == result


def test_star_in_columns_siize():
        

    ddl =   """
    create table ACT_RU_VARIABLE (
        ID_ NVARCHAR2(64) not null,
        REV_ INTEGER,
        TYPE_ NVARCHAR2(255) not null,
        NAME_ NVARCHAR2(255) not null,
        EXECUTION_ID_ NVARCHAR2(64),
        PROC_INST_ID_ NVARCHAR2(64),
        TASK_ID_ NVARCHAR2(64),
        BYTEARRAY_ID_ NVARCHAR2(64),
        DOUBLE_ NUMBER(*,10),
        LONG_ NUMBER(19,0),
        TEXT_ NVARCHAR2(2000),
        TEXT2_ NVARCHAR2(2000),
        primary key (ID_)
    );
    """
    result = DDLParser(ddl).run(group_by_type=True)
    expected = {'ddl_properties': [],
 'domains': [],
 'schemas': [],
 'sequences': [],
 'tables': [{'alter': {},
             'checks': [],
             'columns': [{'check': None,
                          'default': None,
                          'name': 'ID_',
                          'nullable': False,
                          'references': None,
                          'size': 64,
                          'type': 'NVARCHAR2',
                          'unique': False},
                         {'check': None,
                          'default': None,
                          'name': 'REV_',
                          'nullable': True,
                          'references': None,
                          'size': None,
                          'type': 'INTEGER',
                          'unique': False},
                         {'check': None,
                          'default': None,
                          'name': 'TYPE_',
                          'nullable': False,
                          'references': None,
                          'size': 255,
                          'type': 'NVARCHAR2',
                          'unique': False},
                         {'check': None,
                          'default': None,
                          'name': 'NAME_',
                          'nullable': False,
                          'references': None,
                          'size': 255,
                          'type': 'NVARCHAR2',
                          'unique': False},
                         {'check': None,
                          'default': None,
                          'name': 'EXECUTION_ID_',
                          'nullable': True,
                          'references': None,
                          'size': 64,
                          'type': 'NVARCHAR2',
                          'unique': False},
                         {'check': None,
                          'default': None,
                          'name': 'PROC_INST_ID_',
                          'nullable': True,
                          'references': None,
                          'size': 64,
                          'type': 'NVARCHAR2',
                          'unique': False},
                         {'check': None,
                          'default': None,
                          'name': 'TASK_ID_',
                          'nullable': True,
                          'references': None,
                          'size': 64,
                          'type': 'NVARCHAR2',
                          'unique': False},
                         {'check': None,
                          'default': None,
                          'name': 'BYTEARRAY_ID_',
                          'nullable': True,
                          'references': None,
                          'size': 64,
                          'type': 'NVARCHAR2',
                          'unique': False},
                         {'check': None,
                          'default': None,
                          'name': 'DOUBLE_',
                          'nullable': True,
                          'references': None,
                          'size': ('*', 10),
                          'type': 'NUMBER',
                          'unique': False},
                         {'check': None,
                          'default': None,
                          'name': 'LONG_',
                          'nullable': True,
                          'references': None,
                          'size': (19, 0),
                          'type': 'NUMBER',
                          'unique': False},
                         {'check': None,
                          'default': None,
                          'name': 'TEXT_',
                          'nullable': True,
                          'references': None,
                          'size': 2000,
                          'type': 'NVARCHAR2',
                          'unique': False},
                         {'check': None,
                          'default': None,
                          'name': 'TEXT2_',
                          'nullable': True,
                          'references': None,
                          'size': 2000,
                          'type': 'NVARCHAR2',
                          'unique': False}],
             'index': [],
             'partitioned_by': [],
             'primary_key': ['ID_'],
             'schema': None,
             'table_name': 'ACT_RU_VARIABLE',
             'tablespace': None}],
 'types': []}
    assert expected == result


def test_organization_index():
    expected = {'ddl_properties': [],
 'domains': [],
 'schemas': [],
 'sequences': [],
 'tables': [{'alter': {},
             'checks': [],
             'columns': [{'check': None,
                          'default': None,
                          'name': 'parent_criterion_id',
                          'nullable': False,
                          'references': None,
                          'size': 3,
                          'type': 'NUMBER',
                          'unique': False},
                         {'check': None,
                          'default': None,
                          'name': 'child_criterion_id',
                          'nullable': False,
                          'references': None,
                          'size': 3,
                          'type': 'NUMBER',
                          'unique': False},
                         {'check': 'constraint_name statement',
                          'default': None,
                          'name': 'include_exclude_ind',
                          'nullable': False,
                          'references': None,
                          'size': 1,
                          'type': 'CHAR',
                          'unique': False}],
             'constraints': {'primary_keys': [{'columns': ['parent_criterion_id',
                                                           'child_criterion_id'],
                                               'constraint_name': 'pk_meta_criteria_combo'}],
                             'references': [{'columns': [None],
                                             'constraint_name': 'fk_metacritcombo_parent',
                                             'deferrable_initially': None,
                                             'on_delete': 'CASCADE',
                                             'on_update': None,
                                             'schema': None,
                                             'table': 'meta_criteria'},
                                            {'columns': [None],
                                             'constraint_name': 'fk_metacritcombo_child',
                                             'deferrable_initially': None,
                                             'on_delete': None,
                                             'on_update': None,
                                             'schema': None,
                                             'table': 'meta_criteria'}]},
             'index': [],
             'organization_index': True,
             'partitioned_by': [],
             'primary_key': ['parent_criterion_id', 'child_criterion_id'],
             'schema': None,
             'table_name': 'meta_criteria_combo',
             'tablespace': None}],
 'types': []}
    ddl =   """
CREATE TABLE meta_criteria_combo
(
  parent_criterion_id NUMBER(3),
  child_criterion_id  NUMBER(3),
  include_exclude_ind CHAR(1) NOT NULL CONSTRAINT chk_metalistcombo_logicalopr CHECK (include_exclude_ind IN ('I', 'E')),
  CONSTRAINT pk_meta_criteria_combo PRIMARY KEY(parent_criterion_id, child_criterion_id),
  CONSTRAINT fk_metacritcombo_parent FOREIGN KEY(parent_criterion_id) REFERENCES meta_criteria ON DELETE CASCADE, 
  CONSTRAINT fk_metacritcombo_child FOREIGN KEY(child_criterion_id) REFERENCES meta_criteria 
) ORGANIZATION INDEX;

GRANT SELECT ON meta_criteria_combo TO PUBLIC;
""" 
    result = DDLParser(ddl).run(group_by_type=True)
    
    assert result == expected