from simple_ddl_parser import DDLParser


def test_sets_with_dot_and_comma():
    ddl = """
       --
    -- PostgreSQL database dump
    --

    -- Dumped from database version 11.6 (Debian 11.6-1.pgdg90+1)
    -- Dumped by pg_dump version 12.9 (Ubuntu 12.9-0ubuntu0.20.04.1)

    SET statement_timeout = 0;
    SET lock_timeout = 0;
    SET idle_in_transaction_session_timeout = 0;
    SET client_encoding = 'UTF8';
    SET standard_conforming_strings = on;
    SELECT pg_catalog.set_config('search_path', '', false);
    SET check_function_bodies = false;
    SET xmloption = content;
    SET client_min_messages = warning;
    SET row_security = off;

    SET default_tablespace = '';

    --
    -- Name: accounts; Type: TABLE; Schema: public; Owner: myapp
    --


            """
    result = DDLParser(ddl).run(group_by_type=True, output_mode="bigquery")
    expected = {
        "ddl_properties": [
            {"name": "statement_timeout", "value": "0"},
            {"name": "lock_timeout", "value": "0"},
            {"name": "idle_in_transaction_session_timeout", "value": "0"},
            {"name": "client_encoding", "value": "'UTF8'"},
            {"name": "standard_conforming_strings", "value": "on"},
            {"name": "check_function_bodies", "value": "false"},
            {"name": "xmloption", "value": "content"},
            {"name": "client_min_messages", "value": "warning"},
            {"name": "row_security", "value": "off"},
            {"name": "default_tablespace", "value": "''"},
        ],
        "domains": [],
        "schemas": [],
        "sequences": [],
        "tables": [],
        "types": [],
    }
    assert expected == result


def test_parse_validly_tables_after_set():

    ddl = """
        --
    -- PostgreSQL database dump
    --

    -- Dumped from database version 11.6 (Debian 11.6-1.pgdg90+1)
    -- Dumped by pg_dump version 12.9 (Ubuntu 12.9-0ubuntu0.20.04.1)

    SET statement_timeout = 0;
    SET lock_timeout = 0;
    SET idle_in_transaction_session_timeout = 0;
    SET client_encoding = 'UTF8';
    SET standard_conforming_strings = on;
    SELECT pg_catalog.set_config('search_path', '', false);
    SET check_function_bodies = false;
    SET xmloption = content;
    SET client_min_messages = warning;
    SET row_security = off;

    SET default_tablespace = '';

    --
    -- Name: accounts; Type: TABLE; Schema: public; Owner: myapp
    --

    CREATE TABLE public.accounts (
        user_id integer NOT NULL,
        username character varying(50) NOT NULL,
        password character varying(50) NOT NULL,
        email character varying(255) NOT NULL,
    );


            """
    result = DDLParser(ddl).run(group_by_type=True, output_mode="bigquery")
    expected = {
        "ddl_properties": [
            {"name": "statement_timeout", "value": "0"},
            {"name": "lock_timeout", "value": "0"},
            {"name": "idle_in_transaction_session_timeout", "value": "0"},
            {"name": "client_encoding", "value": "'UTF8'"},
            {"name": "standard_conforming_strings", "value": "on"},
            {"name": "check_function_bodies", "value": "false"},
            {"name": "xmloption", "value": "content"},
            {"name": "client_min_messages", "value": "warning"},
            {"name": "row_security", "value": "off"},
            {"name": "default_tablespace", "value": "''"},
        ],
        "domains": [],
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
                        "name": "user_id",
                        "nullable": False,
                        "references": None,
                        "size": None,
                        "type": "integer",
                        "unique": False,
                    },
                    {
                        "check": None,
                        "default": None,
                        "name": "username",
                        "nullable": False,
                        "references": None,
                        "size": 50,
                        "type": "character varying",
                        "unique": False,
                    },
                    {
                        "check": None,
                        "default": None,
                        "name": "password",
                        "nullable": False,
                        "references": None,
                        "size": 50,
                        "type": "character varying",
                        "unique": False,
                    },
                    {
                        "check": None,
                        "default": None,
                        "name": "email",
                        "nullable": False,
                        "references": None,
                        "size": 255,
                        "type": "character varying",
                        "unique": False,
                    },
                ],
                "dataset": "public",
                "index": [],
                "partitioned_by": [],
                "primary_key": [],
                "table_name": "accounts",
                "tablespace": None,
            }
        ],
        "types": [],
    }
    assert expected == result


def test_set_lower_parsed():

    ddl = """

    set hive.enforce.bucketing = true;
        """
    result = DDLParser(ddl).run(group_by_type=True, output_mode="hql")
    expected = {
        "ddl_properties": [{"name": "hive.enforce.bucketing", "value": "true"}],
        "domains": [],
        "schemas": [],
        "sequences": [],
        "tables": [],
        "types": [],
    }
    assert expected == result
