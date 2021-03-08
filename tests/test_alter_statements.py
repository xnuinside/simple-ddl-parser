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
                    "references": None,
                },
                {
                    "name": "description",
                    "type": "varchar",
                    "size": None,
                    "nullable": True,
                    "default": None,
                    "references": None,
                },
                {
                    "name": "link",
                    "type": "varchar",
                    "size": None,
                    "nullable": True,
                    "default": None,
                    "references": None,
                },
                {
                    "name": "created_at",
                    "type": "timestamp",
                    "size": None,
                    "nullable": False,
                    "default": None,
                    "references": None,
                },
                {
                    "name": "updated_at",
                    "type": "timestamp",
                    "size": None,
                    "nullable": False,
                    "default": None,
                    "references": None,
                },
            ],
            "primary_key": ["id"],
            "alter": {},
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
                    "references": None,
                },
                {
                    "name": "attachment_id",
                    "type": "int",
                    "size": None,
                    "nullable": False,
                    "default": None,
                    "references": None,
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
                    "references": None,
                },
                {
                    "name": "title",
                    "type": "varchar",
                    "size": None,
                    "nullable": True,
                    "default": None,
                    "references": None,
                },
                {
                    "name": "description",
                    "type": "varchar",
                    "size": None,
                    "nullable": True,
                    "default": None,
                    "references": None,
                },
                {
                    "name": "created_at",
                    "type": "timestamp",
                    "size": None,
                    "nullable": False,
                    "default": None,
                    "references": None,
                },
                {
                    "name": "updated_at",
                    "type": "timestamp",
                    "size": None,
                    "nullable": False,
                    "default": None,
                    "references": None,
                },
            ],
            "primary_key": ["id"],
            "alter": {},
            "table_name": "attachments",
            "schema": None,
        },
    ]
    parse_results = DDLParser(ddl).run()
    assert expected == parse_results
