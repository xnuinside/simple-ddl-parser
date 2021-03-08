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
            "alter": {
                "columns": [
                    {
                        "name": "material_id",
                        "references": {
                            "column": "id",
                            "table": "materials",
                            "schema": None,
                        },
                    },
                    {
                        "name": "attachment_id",
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
            "alter": {"check": "Age>=18"},
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
            ],
            "primary_key": ["id"],
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
            "alter": {"check": "Age>=18"},
            "checks": [
                {"name": "CHK_Person", "statement": "Age>=18 AND City='Sandnes'"}
            ],
            "table_name": "Persons",
            "schema": None,
        },
    ]

    assert expected == DDLParser(ddl).run()
