from simple_ddl_parser import DDLParser


def test_alter_table_initial_support():
    ddl = """  
        
        CREATE TABLE "materials" (
        "id" int PRIMARY KEY,
        "title" varchar NOT NULL default "New title",
        "description" varchar,
        "link" varchar,
        "created_at" timestamp NOT NULL,
        "updated_at" timestamp NOT NULL
        );

        CREATE TABLE "material_attachments" (
        "material_id" int NOT NULL,
        "attachment_id" int NOT NULL
        );

        CREATE TABLE "attachments" (
        "id" int PRIMARY KEY,
        "title" varchar,
        "description" varchar,
        "created_at" timestamp NOT NULL,
        "updated_at" timestamp NOT NULL
        );
        
        ALTER TABLE "material_attachments" ADD FOREIGN KEY ("material_id") REFERENCES "materials" ("id");

        ALTER TABLE "material_attachments" ADD FOREIGN KEY ("attachment_id") REFERENCES "attachments" ("id");
    """
    expected = [
        {
            "columns": [
                {
                    "name": "id",
                    "type": "int",
                    "size": None,
                    "nullable": False,
                    "default": None,
                    "check": None,
                    "references": None,
                    "unique": False,
                },
                {
                    "name": "title",
                    "type": "varchar",
                    "size": None,
                    "nullable": False,
                    "default": "New title",
                    "check": None,
                    "references": None,
                    "unique": False,
                },
                {
                    "name": "description",
                    "type": "varchar",
                    "size": None,
                    "nullable": True,
                    "default": None,
                    "check": None,
                    "references": None,
                    "unique": False,
                },
                {
                    "name": "link",
                    "type": "varchar",
                    "size": None,
                    "nullable": True,
                    "default": None,
                    "check": None,
                    "references": None,
                    "unique": False,
                },
                {
                    "name": "created_at",
                    "type": "timestamp",
                    "size": None,
                    "nullable": False,
                    "default": None,
                    "check": None,
                    "references": None,
                    "unique": False,
                },
                {
                    "name": "updated_at",
                    "type": "timestamp",
                    "size": None,
                    "nullable": False,
                    "default": None,
                    "check": None,
                    "references": None,
                    "unique": False,
                },
            ],
            "primary_key": ["id"],
            "index": [],
            "alter": {},
            "checks": [],
            "table_name": "materials",
            "schema": None,
        },
        {
            "columns": [
                {
                    "name": "material_id",
                    "type": "int",
                    "size": None,
                    "nullable": False,
                    "default": None,
                    "check": None,
                    "references": None,
                    "unique": False,
                },
                {
                    "name": "attachment_id",
                    "type": "int",
                    "size": None,
                    "nullable": False,
                    "default": None,
                    "check": None,
                    "references": None,
                    "unique": False,
                },
            ],
            "primary_key": [],
            "index": [],
            "alter": {
                "columns": [
                    {
                        "name": "material_id",
                        "constraint_name": None,
                        "references": {
                            "column": "id",
                            "table": "materials",
                            "schema": None,
                        },
                    },
                    {
                        "name": "attachment_id",
                        "constraint_name": None,
                        "references": {
                            "column": "id",
                            "table": "attachments",
                            "schema": None,
                        },
                    },
                ]
            },
            "checks": [],
            "table_name": "material_attachments",
            "schema": None,
        },
        {
            "columns": [
                {
                    "name": "id",
                    "type": "int",
                    "size": None,
                    "nullable": False,
                    "default": None,
                    "check": None,
                    "references": None,
                    "unique": False,
                },
                {
                    "name": "title",
                    "type": "varchar",
                    "size": None,
                    "nullable": True,
                    "default": None,
                    "check": None,
                    "references": None,
                    "unique": False,
                },
                {
                    "name": "description",
                    "type": "varchar",
                    "size": None,
                    "nullable": True,
                    "default": None,
                    "check": None,
                    "references": None,
                    "unique": False,
                },
                {
                    "name": "created_at",
                    "type": "timestamp",
                    "size": None,
                    "nullable": False,
                    "default": None,
                    "check": None,
                    "references": None,
                    "unique": False,
                },
                {
                    "name": "updated_at",
                    "type": "timestamp",
                    "size": None,
                    "nullable": False,
                    "default": None,
                    "check": None,
                    "references": None,
                    "unique": False,
                },
            ],
            "primary_key": ["id"],
            "index": [],
            "alter": {},
            "checks": [],
            "table_name": "attachments",
            "schema": None,
        },
    ]
    parse_results = DDLParser(ddl).run()
    assert expected == parse_results


def test_alter_check():
    ddl = """ 
    CREATE TABLE Persons (
        ID int NOT NULL,
        LastName varchar(255) NOT NULL,
        FirstName varchar(255),
        Age int,
        City varchar(255),
        );

        ALTER TABLE Persons
        ADD CHECK (Age>=18);
    """
    expected = [
        {
            "columns": [
                {
                    "name": "ID",
                    "type": "int",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": False,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "LastName",
                    "type": "varchar",
                    "size": 255,
                    "references": None,
                    "unique": False,
                    "nullable": False,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "FirstName",
                    "type": "varchar",
                    "size": 255,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "Age",
                    "type": "int",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "City",
                    "type": "varchar",
                    "size": 255,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
            ],
            "primary_key": [],
            "index": [],
            "alter": {"checks": [{"constraint_name": None, "statement": ["Age>=18"]}]},
            "checks": [],
            "table_name": "Persons",
            "schema": None,
        }
    ]

    assert DDLParser(ddl).run() == expected


def test_alter_check_combine_all_variants():

    ddl = """
    CREATE TABLE employees (
    id SERIAL PRIMARY KEY,
    first_name VARCHAR (50) default 'User Name',
    last_name VARCHAR (50) default 'User Last Name',
    birth_date DATE CHECK (birth_date > '1900-01-01'),
    joined_date DATE CHECK (joined_date > birth_date),
    salary numeric CHECK(salary > 0)
    );
    CREATE TABLE Persons (
    ID int NOT NULL,
    LastName varchar(255) NOT NULL,
    FirstName varchar(255),
    Age int,
    City varchar(255),
    CONSTRAINT CHK_Person CHECK (Age>=18 AND City='Sandnes')
    );

    ALTER TABLE Persons
    ADD CHECK (Age>=18);
    """
    expected = [
        {
            "columns": [
                {
                    "name": "id",
                    "type": "SERIAL",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": False,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "first_name",
                    "type": "VARCHAR",
                    "size": 50,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": "User Name",
                    "check": None,
                },
                {
                    "name": "last_name",
                    "type": "VARCHAR",
                    "size": 50,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": "User Last Name",
                    "check": None,
                },
                {
                    "name": "birth_date",
                    "type": "DATE",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": "birth_date > '1900-01-01'",
                },
                {
                    "name": "joined_date",
                    "type": "DATE",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": "joined_date > birth_date",
                },
                {
                    "check": "salary > 0",
                    "default": None,
                    "name": "salary",
                    "nullable": True,
                    "references": None,
                    "size": None,
                    "type": "numeric",
                    "unique": False,
                },
            ],
            "primary_key": ["id"],
            "index": [],
            "alter": {},
            "checks": [],
            "table_name": "employees",
            "schema": None,
        },
        {
            "columns": [
                {
                    "name": "ID",
                    "type": "int",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": False,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "LastName",
                    "type": "varchar",
                    "size": 255,
                    "references": None,
                    "unique": False,
                    "nullable": False,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "FirstName",
                    "type": "varchar",
                    "size": 255,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "Age",
                    "type": "int",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "City",
                    "type": "varchar",
                    "size": 255,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
            ],
            "primary_key": [],
            "index": [],
            "alter": {"checks": [{"constraint_name": None, "statement": ["Age>=18"]}]},
            "checks": [
                {
                    "constraint_name": "CHK_Person",
                    "statement": "Age>=18 AND City='Sandnes'",
                }
            ],
            "table_name": "Persons",
            "schema": None,
        },
    ]

    assert expected == DDLParser(ddl).run()


def test_alter_check_with_constraint():
    parse_results = DDLParser(
        """

        CREATE TABLE Persons (
        ID int NOT NULL,
        LastName varchar(255) NOT NULL,
        FirstName varchar(255),
        Age int,
        City varchar(255),

        );
        Alter Table Persons ADD CONSTRAINT CHK_PersonAge CHECK (Age>=18 AND City='Sandnes');   
    """
    ).run()
    expected = [
        {
            "columns": [
                {
                    "name": "ID",
                    "type": "int",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": False,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "LastName",
                    "type": "varchar",
                    "size": 255,
                    "references": None,
                    "unique": False,
                    "nullable": False,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "FirstName",
                    "type": "varchar",
                    "size": 255,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "Age",
                    "type": "int",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "City",
                    "type": "varchar",
                    "size": 255,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
            ],
            "primary_key": [],
            "index": [],
            "alter": {
                "checks": [
                    {
                        "constraint_name": "CHK_PersonAge",
                        "statement": ["Age>=18", "AND", "City='Sandnes'"],
                    }
                ]
            },
            "checks": [],
            "table_name": "Persons",
            "schema": None,
        }
    ]
    assert expected == parse_results


def test_alter_foreiggn_with_constraint():
    parse_results = DDLParser(
        """
        CREATE TABLE Persons (
        ID int NOT NULL,
        LastName varchar(255) NOT NULL,
        FirstName varchar(255),
        Age int,
        City varchar(255),
        CONSTRAINT CHK_Person CHECK (Age>=18 AND City='Sandnes')
        );

        Alter Table Persons ADD CONSTRAINT fk_group FOREIGN KEY (id) REFERENCES employees (id); 


        """
    ).run()
    expected = [
        {
            "columns": [
                {
                    "name": "ID",
                    "type": "int",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": False,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "LastName",
                    "type": "varchar",
                    "size": 255,
                    "references": None,
                    "unique": False,
                    "nullable": False,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "FirstName",
                    "type": "varchar",
                    "size": 255,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "Age",
                    "type": "int",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "City",
                    "type": "varchar",
                    "size": 255,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
            ],
            "primary_key": [],
            "index": [],
            "alter": {
                "columns": [
                    {
                        "name": "id",
                        "constraint_name": "fk_group",
                        "references": {
                            "column": "id",
                            "table": "employees",
                            "schema": None,
                        },
                    }
                ]
            },
            "checks": [
                {
                    "constraint_name": "CHK_Person",
                    "statement": "Age>=18 AND City='Sandnes'",
                }
            ],
            "table_name": "Persons",
            "schema": None,
        }
    ]
    assert parse_results == expected


def test_alter_without_constraint_and_constraint_in_table():
    parse_results = DDLParser(
        """
        CREATE TABLE Persons (
        ID int NOT NULL,
        LastName varchar(255) NOT NULL,
        FirstName varchar(255),
        Age int,
        City varchar(255),
        CONSTRAINT CHK_Person CHECK (Age>=18 AND City='Sandnes')
        );

        ALTER TABLE Persons ADD CHECK (Age>=18 AND City='Sandnes');

        """
    ).run()
    expected = [
        {
            "columns": [
                {
                    "name": "ID",
                    "type": "int",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": False,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "LastName",
                    "type": "varchar",
                    "size": 255,
                    "references": None,
                    "unique": False,
                    "nullable": False,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "FirstName",
                    "type": "varchar",
                    "size": 255,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "Age",
                    "type": "int",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "City",
                    "type": "varchar",
                    "size": 255,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
            ],
            "primary_key": [],
            "index": [],
            "alter": {
                "checks": [
                    {
                        "constraint_name": None,
                        "statement": ["Age>=18", "AND", "City='Sandnes'"],
                    }
                ]
            },
            "checks": [
                {
                    "constraint_name": "CHK_Person",
                    "statement": "Age>=18 AND City='Sandnes'",
                }
            ],
            "table_name": "Persons",
            "schema": None,
        }
    ]
    assert expected == parse_results


def test_combo_with_alter_and_table_constraints():
    parse_results = DDLParser(
        """
    CREATE TABLE employees (
        id SERIAL PRIMARY KEY,
        first_name VARCHAR (50),
        last_name VARCHAR (50),
        birth_date DATE CHECK (birth_date > '1900-01-01'),
        joined_date DATE CHECK (joined_date > birth_date),
        salary numeric CHECK(salary > 0)
    );
    CREATE TABLE Persons (
        ID int NOT NULL,
        LastName varchar(255) NOT NULL,
        FirstName varchar(255),
        Age int,
        City varchar(255),
        CONSTRAINT CHK_Person CHECK (Age>=18 AND City='Sandnes')
    );
    
    ALTER TABLE Persons ADD CHECK (Age>=18 AND City='Sandnes);
    
    ALTER TABLE Persons Add CONSTRAINT ck_person  CHECK (Age>=18 AND City='Sandnes);
    Alter Table Persons ADD CONSTRAINT fk_group FOREIGN KEY (id) REFERENCES employees (id); 
        """
    ).run()

    expected = [
        {
            "columns": [
                {
                    "name": "id",
                    "type": "SERIAL",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": False,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "first_name",
                    "type": "VARCHAR",
                    "size": 50,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "last_name",
                    "type": "VARCHAR",
                    "size": 50,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "birth_date",
                    "type": "DATE",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": "birth_date > '1900-01-01'",
                },
                {
                    "name": "joined_date",
                    "type": "DATE",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": "joined_date > birth_date",
                },
                {
                    "check": "salary > 0",
                    "default": None,
                    "name": "salary",
                    "nullable": True,
                    "references": None,
                    "size": None,
                    "type": "numeric",
                    "unique": False,
                },
            ],
            "primary_key": ["id"],
            "index": [],
            "alter": {},
            "checks": [],
            "table_name": "employees",
            "schema": None,
        },
        {
            "columns": [
                {
                    "name": "ID",
                    "type": "int",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": False,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "LastName",
                    "type": "varchar",
                    "size": 255,
                    "references": None,
                    "unique": False,
                    "nullable": False,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "FirstName",
                    "type": "varchar",
                    "size": 255,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "Age",
                    "type": "int",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "City",
                    "type": "varchar",
                    "size": 255,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
            ],
            "primary_key": [],
            "index": [],
            "alter": {
                "checks": [
                    {
                        "constraint_name": None,
                        "statement": ["Age>=18", "AND", "City='Sandnes"],
                    },
                    {
                        "constraint_name": "ck_person",
                        "statement": ["Age>=18", "AND", "City='Sandnes"],
                    },
                ],
                "columns": [
                    {
                        "name": "id",
                        "constraint_name": "fk_group",
                        "references": {
                            "column": "id",
                            "table": "employees",
                            "schema": None,
                        },
                    }
                ],
            },
            "checks": [
                {
                    "constraint_name": "CHK_Person",
                    "statement": "Age>=18 AND City='Sandnes'",
                }
            ],
            "table_name": "Persons",
            "schema": None,
        },
    ]
    assert expected == parse_results


def test_alter_foreign_with_multiple_ids():
    parse_results = DDLParser(
        """
CREATE TABLE employees (
            id SERIAL PRIMARY KEY,
            first_name VARCHAR (50),
            last_name VARCHAR (50),
            birth_date DATE CHECK (birth_date > '1900-01-01'),
            joined_date DATE CHECK (joined_date > birth_date),
            salary numeric CHECK(salary > 0)
            Age int,
        );
        CREATE TABLE Persons (
            ID int NOT NULL,
            LastName varchar(255) NOT NULL,
            FirstName varchar(255),
            Age int,
            City varchar(255),
            birth_date DATE CHECK (birth_date > '1900-01-01'),
            CONSTRAINT CHK_Person CHECK (Age>=18 AND City='Sandnes')
        );
        Alter Table Persons ADD CONSTRAINT fk_group FOREIGN KEY (id, Age, birth_date) REFERENCES employees (id, Age, birth_date);          
"""
    ).run()
    expected = [
        {
            "columns": [
                {
                    "name": "id",
                    "type": "SERIAL",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": False,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "first_name",
                    "type": "VARCHAR",
                    "size": 50,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "last_name",
                    "type": "VARCHAR",
                    "size": 50,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "birth_date",
                    "type": "DATE",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": "birth_date > '1900-01-01'",
                },
                {
                    "name": "joined_date",
                    "type": "DATE",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": "joined_date > birth_date",
                },
                {
                    "name": "salary",
                    "type": "numeric",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": "salary > 0 Age int",
                },
            ],
            "primary_key": ["id"],
            "alter": {},
            "checks": [],
            "index": [],
            "schema": None,
            "table_name": "employees",
        },
        {
            "columns": [
                {
                    "name": "ID",
                    "type": "int",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": False,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "LastName",
                    "type": "varchar",
                    "size": 255,
                    "references": None,
                    "unique": False,
                    "nullable": False,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "FirstName",
                    "type": "varchar",
                    "size": 255,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "Age",
                    "type": "int",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "City",
                    "type": "varchar",
                    "size": 255,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": None,
                },
                {
                    "name": "birth_date",
                    "type": "DATE",
                    "size": None,
                    "references": None,
                    "unique": False,
                    "nullable": True,
                    "default": None,
                    "check": "birth_date > '1900-01-01'",
                },
            ],
            "primary_key": [],
            "alter": {
                "columns": [
                    {
                        "name": "id",
                        "constraint_name": "fk_group",
                        "references": {
                            "column": "id",
                            "table": "employees",
                            "schema": None,
                        },
                    },
                    {
                        "name": "Age",
                        "constraint_name": "fk_group",
                        "references": {
                            "column": "Age",
                            "table": "employees",
                            "schema": None,
                        },
                    },
                    {
                        "name": "birth_date",
                        "constraint_name": "fk_group",
                        "references": {
                            "column": "birth_date",
                            "table": "employees",
                            "schema": None,
                        },
                    },
                ]
            },
            "checks": [
                {
                    "constraint_name": "CHK_Person",
                    "statement": "Age>=18 AND City='Sandnes'",
                }
            ],
            "index": [],
            "schema": None,
            "table_name": "Persons",
        },
    ]
    assert expected == parse_results

    def test_several_alter_fk_for_same_table():
        parse_results = DDLParser(
            """
CREATE TABLE employees (
            id SERIAL PRIMARY KEY,
            first_name VARCHAR (50),
            last_name VARCHAR (50),
            birth_date DATE CHECK (birth_date > '1900-01-01'),
            joined_date DATE CHECK (joined_date > birth_date),
            salary numeric CHECK(salary > 0)
            Age int,
        );
        CREATE TABLE Persons (
            ID int NOT NULL,
            LastName varchar(255) NOT NULL,
            FirstName varchar(255),
            Age int,
            City varchar(255),
            birth_date DATE CHECK (birth_date > '1900-01-01'),
            CONSTRAINT CHK_Person CHECK (Age>=18 AND City='Sandnes')
        );
        Alter Table Persons ADD CONSTRAINT fk_group FOREIGN KEY (id, Age) REFERENCES employees (id, Age, birth_date);          
        Alter Table Persons ADD CONSTRAINT new_fk FOREIGN KEY (birth_date) REFERENCES employees (birth_date);          
"""
        ).run()
        expected = [
            {
                "columns": [
                    {
                        "name": "id",
                        "type": "SERIAL",
                        "size": None,
                        "references": None,
                        "unique": False,
                        "nullable": False,
                        "default": None,
                        "check": None,
                    },
                    {
                        "name": "first_name",
                        "type": "VARCHAR",
                        "size": 50,
                        "references": None,
                        "unique": False,
                        "nullable": True,
                        "default": None,
                        "check": None,
                    },
                    {
                        "name": "last_name",
                        "type": "VARCHAR",
                        "size": 50,
                        "references": None,
                        "unique": False,
                        "nullable": True,
                        "default": None,
                        "check": None,
                    },
                    {
                        "name": "birth_date",
                        "type": "DATE",
                        "size": None,
                        "references": None,
                        "unique": False,
                        "nullable": True,
                        "default": None,
                        "check": "birth_date > '1900-01-01'",
                    },
                    {
                        "name": "joined_date",
                        "type": "DATE",
                        "size": None,
                        "references": None,
                        "unique": False,
                        "nullable": True,
                        "default": None,
                        "check": "joined_date > birth_date",
                    },
                    {
                        "name": "salary",
                        "type": "numeric",
                        "size": None,
                        "references": None,
                        "unique": False,
                        "nullable": True,
                        "default": None,
                        "check": "salary > 0 Age int",
                    },
                ],
                "primary_key": ["id"],
                "alter": {},
                "checks": [],
                "index": [],
                "schema": None,
                "table_name": "employees",
            },
            {
                "columns": [
                    {
                        "name": "ID",
                        "type": "int",
                        "size": None,
                        "references": None,
                        "unique": False,
                        "nullable": False,
                        "default": None,
                        "check": None,
                    },
                    {
                        "name": "LastName",
                        "type": "varchar",
                        "size": 255,
                        "references": None,
                        "unique": False,
                        "nullable": False,
                        "default": None,
                        "check": None,
                    },
                    {
                        "name": "FirstName",
                        "type": "varchar",
                        "size": 255,
                        "references": None,
                        "unique": False,
                        "nullable": True,
                        "default": None,
                        "check": None,
                    },
                    {
                        "name": "Age",
                        "type": "int",
                        "size": None,
                        "references": None,
                        "unique": False,
                        "nullable": True,
                        "default": None,
                        "check": None,
                    },
                    {
                        "name": "City",
                        "type": "varchar",
                        "size": 255,
                        "references": None,
                        "unique": False,
                        "nullable": True,
                        "default": None,
                        "check": None,
                    },
                    {
                        "name": "birth_date",
                        "type": "DATE",
                        "size": None,
                        "references": None,
                        "unique": False,
                        "nullable": True,
                        "default": None,
                        "check": "birth_date > '1900-01-01'",
                    },
                ],
                "primary_key": [],
                "alter": {
                    "columns": [
                        {
                            "name": "id",
                            "constraint_name": "fk_group",
                            "references": {
                                "column": "id",
                                "table": "employees",
                                "schema": None,
                            },
                        },
                        {
                            "name": "Age",
                            "constraint_name": "fk_group",
                            "references": {
                                "column": "Age",
                                "table": "employees",
                                "schema": None,
                            },
                        },
                        {
                            "name": "birth_date",
                            "constraint_name": "new_fk",
                            "references": {
                                "column": "birth_date",
                                "table": "employees",
                                "schema": None,
                            },
                        },
                    ]
                },
                "checks": [
                    {
                        "constraint_name": "CHK_Person",
                        "statement": "Age>=18 AND City='Sandnes'",
                    }
                ],
                "index": [],
                "schema": None,
                "table_name": "Persons",
            },
        ]
        assert expected == parse_results
