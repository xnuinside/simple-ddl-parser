import os
from simple_ddl_parser import DDLParser, parse_from_file


def test_run_postgres_first_query():
    ddl = """
    create table prod.super_table
(
    data_sync_id bigint not null,
    sync_count bigint not null,
    sync_mark timestamp  not  null,
    sync_start timestamp  not null,
    sync_end timestamp  not null,
    message varchar(2000) null,
    primary key (data_sync_id, sync_start)
);
    """
    expected = {
        'columns': [
            {'name': 'data_sync_id', 'type': 'bigint', 'nullable': False, 'size': None}, 
            {'name': 'sync_count', 'type': 'bigint', 'nullable': False, 'size': None}, 
            {'name': 'sync_mark', 'type': 'timestamp', 'nullable': False, 'size': None}, 
            {'name': 'sync_start', 'type': 'timestamp', 'nullable': False, 'size': None}, 
            {'name': 'sync_end', 'type': 'timestamp', 'nullable': False, 'size': None}, 
            {'name': 'message', 'type': 'varchar', 'nullable': False, 'size': 2000, }], 
        'table_name': 'super_table', 'schema': 'prod', 
        'primary_key': ['data_sync_id', 'sync_start']
        }
    assert expected == DDLParser(ddl).run()


def test_run_hql_query():
    ddl = """
    CREATE TABLE "paths" (
        "id" int PRIMARY KEY,
        "title" varchar NOT NULL,
        "description" varchar(160),
        "created_at" timestamp,
        "updated_at" timestamp
    );
    """
    expected = {
        'columns': [
            {'name': 'id', 'type': 'int', 'nullable': False, 'size': None}, 
            {'name': 'title', 'type': 'varchar', 'nullable': False, 'size': None}, 
            {'name': 'description', 'type': 'varchar', 'nullable': False, 'size': 160}, 
            {'name': 'created_at', 'type': 'timestamp', 'nullable': False, 'size': None}, 
            {'name': 'updated_at', 'type': 'timestamp', 'nullable': False, 'size': None}], 
        'primary_key': ['id'], 'table_name': 'paths', 'schema': None}
    assert expected == DDLParser(ddl).run()



def test_run_hql_query_caps_in_columns():
    ddl = """
    CREATE TABLE "paths" (
        "ID" int PRIMARY KEY,
        "TITLE" varchar NOT NULL,
        "description" varchar(160),
        "created_at" timestamp,
        "updated_at" timestamp
    );
    """
    expected = {
        'columns': [
            {'name': 'ID', 'type': 'int', 'nullable': False, 'size': None}, 
            {'name': 'TITLE', 'type': 'varchar', 'nullable': False, 'size': None}, 
            {'name': 'description', 'type': 'varchar', 'nullable': False, 'size': 160}, 
            {'name': 'created_at', 'type': 'timestamp', 'nullable': False, 'size': None}, 
            {'name': 'updated_at', 'type': 'timestamp', 'nullable': False, 'size': None}], 
        'primary_key': ['ID'], 'table_name': 'paths', 'schema': None}
    assert expected == DDLParser(ddl).run()


def test_parse_from_file_one_table():
    expected = {
        'columns': [
            {'name': 'id', 'type': 'SERIAL', 'size': None, 'nullable': False}, 
            {'name': 'name', 'type': 'varchar', 'size': 160, 'nullable': False}, 
            {'name': 'created_at', 'type': 'timestamp', 'size': None, 'nullable': False}, 
            {'name': 'updated_at', 'type': 'timestamp', 'size': None, 'nullable': False}, 
            {'name': 'country_code', 'type': 'int', 'size': None, 'nullable': False}, 
            {'name': 'default_language', 'type': 'int', 'size': None, 'nullable': False}], 
        'primary_key': ['id'], 
        'table_name': 'users', 
        'schema': None}
    current_path = os.path.dirname(os.path.abspath(__file__))
    assert expected == parse_from_file(os.path.join(current_path, 'test_one_statement.sql'))
