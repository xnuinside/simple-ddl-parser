"""
Tests for Informix/GBase 8s DDL syntax.

Informix is an IBM relational database management system.
GBase 8s is a Chinese enterprise database based on Informix.

References:
- https://www.ibm.com/docs/en/informix-servers/14.10.0
- https://www.oninit.com/manual/informix/
"""

from simple_ddl_parser import DDLParser


def test_informix_basic_table():
    """Test basic Informix CREATE TABLE with common data types."""
    ddl = """
    CREATE TABLE people (
        id serial PRIMARY KEY,
        name varchar(20),
        age int CHECK (age > 0),
        sex char(1) CHECK (sex = 'M' OR sex = 'F'),
        birthday date,
        working boolean,
        salary money(16,2),
        descript text
    );
    """
    result = DDLParser(ddl).run(group_by_type=True)

    assert len(result["tables"]) == 1
    table = result["tables"][0]
    assert table["table_name"] == "people"
    assert table["primary_key"] == ["id"]

    columns = {c["name"]: c for c in table["columns"]}
    assert columns["id"]["type"] == "serial"
    assert columns["name"]["type"] == "varchar"
    assert columns["name"]["size"] == 20
    assert columns["age"]["type"] == "int"
    assert columns["sex"]["type"] == "char"
    assert columns["birthday"]["type"] == "date"
    assert columns["working"]["type"] == "boolean"
    assert columns["salary"]["type"] == "money"
    assert columns["descript"]["type"] == "text"


def test_informix_serial_types():
    """Test Informix SERIAL, SERIAL8, and BIGSERIAL data types."""
    ddl = """
    CREATE TABLE orders (
        order_id SERIAL NOT NULL,
        order_id_8 SERIAL8,
        order_id_big BIGSERIAL,
        order_date DATE
    );
    """
    result = DDLParser(ddl).run(group_by_type=True)

    table = result["tables"][0]
    columns = {c["name"]: c for c in table["columns"]}

    assert columns["order_id"]["type"] == "SERIAL"
    assert columns["order_id"]["nullable"] is False
    assert columns["order_id_8"]["type"] == "SERIAL8"
    assert columns["order_id_big"]["type"] == "BIGSERIAL"


def test_informix_datetime_year_to_second():
    """Test Informix DATETIME YEAR TO SECOND data type."""
    ddl = """
    CREATE TABLE events (
        event_id INT PRIMARY KEY,
        event_time DATETIME YEAR TO SECOND,
        created_at DATETIME YEAR TO FRACTION(5),
        event_date DATETIME YEAR TO DAY
    );
    """
    result = DDLParser(ddl).run(group_by_type=True)

    table = result["tables"][0]
    columns = {c["name"]: c for c in table["columns"]}

    assert columns["event_time"]["type"] == "DATETIME YEAR TO SECOND"
    assert columns["created_at"]["type"] == "DATETIME YEAR TO FRACTION"
    assert columns["event_date"]["type"] == "DATETIME YEAR TO DAY"


def test_informix_interval_types():
    """Test Informix INTERVAL data types."""
    ddl = """
    CREATE TABLE durations (
        id INT,
        work_hours INTERVAL HOUR TO MINUTE,
        vacation_days INTERVAL DAY TO DAY,
        work_period INTERVAL YEAR TO MONTH
    );
    """
    result = DDLParser(ddl).run(group_by_type=True)

    table = result["tables"][0]
    columns = {c["name"]: c for c in table["columns"]}

    assert columns["work_hours"]["type"] == "INTERVAL HOUR TO MINUTE"
    assert columns["vacation_days"]["type"] == "INTERVAL DAY TO DAY"
    assert columns["work_period"]["type"] == "INTERVAL YEAR TO MONTH"


def test_informix_lvarchar_text_byte():
    """Test Informix LVARCHAR, TEXT, and BYTE data types."""
    ddl = """
    CREATE TABLE documents (
        doc_id SERIAL PRIMARY KEY,
        title LVARCHAR(500),
        content TEXT,
        binary_data BYTE
    );
    """
    result = DDLParser(ddl).run(group_by_type=True)

    table = result["tables"][0]
    columns = {c["name"]: c for c in table["columns"]}

    assert columns["title"]["type"] == "LVARCHAR"
    assert columns["title"]["size"] == 500
    assert columns["content"]["type"] == "TEXT"
    assert columns["binary_data"]["type"] == "BYTE"


def test_informix_blob_clob():
    """Test Informix BLOB and CLOB data types."""
    ddl = """
    CREATE TABLE media (
        id INT PRIMARY KEY,
        image BLOB,
        document CLOB
    );
    """
    result = DDLParser(ddl).run(group_by_type=True)

    table = result["tables"][0]
    columns = {c["name"]: c for c in table["columns"]}

    assert columns["image"]["type"] == "BLOB"
    assert columns["document"]["type"] == "CLOB"


def test_informix_default_functions():
    """Test Informix DEFAULT with built-in functions: TODAY, CURRENT, USER."""
    ddl = """
    CREATE TABLE audit_log (
        log_id SERIAL PRIMARY KEY,
        log_date DATE DEFAULT TODAY,
        log_time DATETIME YEAR TO SECOND DEFAULT CURRENT,
        log_user VARCHAR(32) DEFAULT USER
    );
    """
    result = DDLParser(ddl).run(group_by_type=True)

    table = result["tables"][0]
    columns = {c["name"]: c for c in table["columns"]}

    assert columns["log_date"]["default"] == "TODAY"
    assert columns["log_time"]["default"] == "CURRENT"
    assert columns["log_user"]["default"] == "USER"


def test_informix_check_constraints():
    """Test Informix CHECK constraints (column-level and table-level)."""
    ddl = """
    CREATE TABLE employees (
        emp_id INT NOT NULL,
        salary MONEY(16,2) CHECK (salary > 0),
        bonus MONEY(10,2),
        CHECK (bonus <= salary)
    );
    """
    result = DDLParser(ddl).run(group_by_type=True)

    table = result["tables"][0]
    # Table-level CHECK
    assert len(table["checks"]) == 1
    assert "bonus <= salary" in table["checks"][0]["statement"]


def test_informix_storage_options():
    """Test Informix storage options: IN dbspace, EXTENT SIZE, NEXT SIZE, LOCK MODE."""
    ddl = """
    CREATE TABLE role (
        role_id INTEGER NOT NULL,
        role_nm VARCHAR(10) NOT NULL,
        role_desc VARCHAR(20) NOT NULL,
        PRIMARY KEY (role_id)
    ) IN devdata EXTENT SIZE 32 NEXT SIZE 32 LOCK MODE page;
    """
    result = DDLParser(ddl).run(group_by_type=True)

    table = result["tables"][0]
    assert table["table_name"] == "role"
    assert table["primary_key"] == ["role_id"]
    # Storage options are captured in table_properties
    assert "table_properties" in table


def test_informix_money_type():
    """Test Informix MONEY data type with precision."""
    ddl = """
    CREATE TABLE transactions (
        trans_id SERIAL PRIMARY KEY,
        amount MONEY(16,2) NOT NULL,
        tax MONEY(10,2),
        total MONEY
    );
    """
    result = DDLParser(ddl).run(group_by_type=True)

    table = result["tables"][0]
    columns = {c["name"]: c for c in table["columns"]}

    assert columns["amount"]["type"] == "MONEY"
    assert columns["tax"]["type"] == "MONEY"
    assert columns["total"]["type"] == "MONEY"


def test_informix_nchar_nvarchar():
    """Test Informix NCHAR and NVARCHAR (national character) data types."""
    ddl = """
    CREATE TABLE i18n_data (
        id INT PRIMARY KEY,
        name NCHAR(50),
        description NVARCHAR(255)
    );
    """
    result = DDLParser(ddl).run(group_by_type=True)

    table = result["tables"][0]
    columns = {c["name"]: c for c in table["columns"]}

    assert columns["name"]["type"] == "NCHAR"
    assert columns["name"]["size"] == 50
    assert columns["description"]["type"] == "NVARCHAR"
    assert columns["description"]["size"] == 255


def test_informix_int8():
    """Test Informix INT8 (64-bit integer) data type."""
    ddl = """
    CREATE TABLE big_numbers (
        id INT PRIMARY KEY,
        big_value INT8,
        counter BIGINT
    );
    """
    result = DDLParser(ddl).run(group_by_type=True)

    table = result["tables"][0]
    columns = {c["name"]: c for c in table["columns"]}

    assert columns["big_value"]["type"] == "INT8"
    assert columns["counter"]["type"] == "BIGINT"


def test_informix_foreign_key():
    """Test Informix FOREIGN KEY constraint."""
    ddl = """
    CREATE TABLE orders (
        order_id SERIAL PRIMARY KEY,
        customer_id INT NOT NULL,
        FOREIGN KEY (customer_id) REFERENCES customers(id)
    );
    """
    result = DDLParser(ddl).run(group_by_type=True)

    table = result["tables"][0]
    columns = {c["name"]: c for c in table["columns"]}

    assert columns["customer_id"]["references"]["table"] == "customers"
    assert columns["customer_id"]["references"]["column"] == "id"


def test_informix_unique_constraint():
    """Test Informix UNIQUE constraint."""
    ddl = """
    CREATE TABLE users (
        user_id SERIAL PRIMARY KEY,
        email VARCHAR(100) UNIQUE,
        username VARCHAR(50),
        UNIQUE (username)
    );
    """
    result = DDLParser(ddl).run(group_by_type=True)

    table = result["tables"][0]
    columns = {c["name"]: c for c in table["columns"]}

    assert columns["email"]["unique"] is True


def test_informix_composite_primary_key():
    """Test Informix composite PRIMARY KEY."""
    ddl = """
    CREATE TABLE order_items (
        order_id INT NOT NULL,
        item_id INT NOT NULL,
        quantity INT,
        price MONEY(10,2),
        PRIMARY KEY (order_id, item_id)
    );
    """
    result = DDLParser(ddl).run(group_by_type=True)

    table = result["tables"][0]
    assert table["primary_key"] == ["order_id", "item_id"]


def test_informix_smallfloat_real():
    """Test Informix SMALLFLOAT and REAL data types."""
    ddl = """
    CREATE TABLE measurements (
        id INT PRIMARY KEY,
        value1 SMALLFLOAT,
        value2 REAL,
        value3 DOUBLE PRECISION
    );
    """
    result = DDLParser(ddl).run(group_by_type=True)

    table = result["tables"][0]
    columns = {c["name"]: c for c in table["columns"]}

    assert columns["value1"]["type"] == "SMALLFLOAT"
    assert columns["value2"]["type"] == "REAL"
    assert columns["value3"]["type"] == "DOUBLE PRECISION"


def test_informix_decimal_numeric():
    """Test Informix DECIMAL and NUMERIC data types."""
    ddl = """
    CREATE TABLE financial (
        id INT PRIMARY KEY,
        amount DECIMAL(10,2),
        rate NUMERIC(5,4),
        balance DEC(12,2)
    );
    """
    result = DDLParser(ddl).run(group_by_type=True)

    table = result["tables"][0]
    columns = {c["name"]: c for c in table["columns"]}

    assert columns["amount"]["type"] == "DECIMAL"
    assert columns["rate"]["type"] == "NUMERIC"
    assert columns["balance"]["type"] == "DEC"


def test_informix_smallint_integer():
    """Test Informix SMALLINT and INTEGER data types."""
    ddl = """
    CREATE TABLE counters (
        id INTEGER PRIMARY KEY,
        small_count SMALLINT,
        big_count INT
    );
    """
    result = DDLParser(ddl).run(group_by_type=True)

    table = result["tables"][0]
    columns = {c["name"]: c for c in table["columns"]}

    assert columns["id"]["type"] == "INTEGER"
    assert columns["small_count"]["type"] == "SMALLINT"
    assert columns["big_count"]["type"] == "INT"


def test_informix_multiple_tables():
    """Test parsing multiple Informix tables."""
    ddl = """
    CREATE TABLE customers (
        customer_id SERIAL PRIMARY KEY,
        name VARCHAR(100) NOT NULL,
        email VARCHAR(100)
    );

    CREATE TABLE orders (
        order_id SERIAL PRIMARY KEY,
        customer_id INT NOT NULL,
        order_date DATE DEFAULT TODAY,
        total MONEY(16,2)
    );

    CREATE TABLE order_items (
        order_id INT NOT NULL,
        product_id INT NOT NULL,
        quantity INT DEFAULT 1,
        PRIMARY KEY (order_id, product_id)
    );
    """
    result = DDLParser(ddl).run(group_by_type=True)

    assert len(result["tables"]) == 3
    table_names = [t["table_name"] for t in result["tables"]]
    assert "customers" in table_names
    assert "orders" in table_names
    assert "order_items" in table_names


# ============================================================================
# GBase 8s specific tests
# GBase 8s is based on Informix but adds Oracle-compatible features
# ============================================================================


def test_gbase8s_varchar2():
    """Test GBase 8s VARCHAR2 data type (Oracle compatibility)."""
    ddl = """
    CREATE TABLE employees (
        emp_id INT PRIMARY KEY,
        first_name VARCHAR2(50),
        last_name VARCHAR2(50),
        email VARCHAR2(100)
    );
    """
    result = DDLParser(ddl).run(group_by_type=True)

    table = result["tables"][0]
    columns = {c["name"]: c for c in table["columns"]}

    assert columns["first_name"]["type"] == "VARCHAR2"
    assert columns["first_name"]["size"] == 50
    assert columns["email"]["type"] == "VARCHAR2"
    assert columns["email"]["size"] == 100


def test_gbase8s_number_numeric():
    """Test GBase 8s NUMBER/NUMERIC data types (Oracle compatibility)."""
    ddl = """
    CREATE TABLE financial_data (
        id INT PRIMARY KEY,
        amount NUMBER(10,2),
        rate NUMERIC(5,4),
        quantity NUMBER
    );
    """
    result = DDLParser(ddl).run(group_by_type=True)

    table = result["tables"][0]
    columns = {c["name"]: c for c in table["columns"]}

    assert columns["amount"]["type"] == "NUMBER"
    assert columns["rate"]["type"] == "NUMERIC"
    assert columns["quantity"]["type"] == "NUMBER"


def test_gbase8s_virtual_column_as():
    """Test GBase 8s virtual column with AS syntax."""
    ddl = """
    CREATE TABLE products (
        product_id INT PRIMARY KEY,
        price DECIMAL(10,2),
        quantity INT,
        total_value DECIMAL(12,2) AS (price * quantity)
    );
    """
    result = DDLParser(ddl).run(group_by_type=True)

    table = result["tables"][0]
    columns = {c["name"]: c for c in table["columns"]}

    assert columns["total_value"]["type"] == "DECIMAL"
    assert "generated" in columns["total_value"]
    assert columns["total_value"]["generated"]["as"] == "price * quantity"


def test_gbase8s_virtual_column_generated_always():
    """Test GBase 8s virtual column with GENERATED ALWAYS AS syntax."""
    ddl = """
    CREATE TABLE orders (
        order_id INT PRIMARY KEY,
        subtotal DECIMAL(10,2),
        tax_rate DECIMAL(4,2),
        tax_amount DECIMAL(10,2) GENERATED ALWAYS AS (subtotal * tax_rate)
    );
    """
    result = DDLParser(ddl).run(group_by_type=True)

    table = result["tables"][0]
    columns = {c["name"]: c for c in table["columns"]}

    assert columns["tax_amount"]["type"] == "DECIMAL"
    assert "generated" in columns["tax_amount"]


def test_gbase8s_nvarchar2():
    """Test GBase 8s NVARCHAR2 data type (Oracle compatibility)."""
    ddl = """
    CREATE TABLE i18n_content (
        id INT PRIMARY KEY,
        title NVARCHAR2(100),
        description NVARCHAR2(500)
    );
    """
    result = DDLParser(ddl).run(group_by_type=True)

    table = result["tables"][0]
    columns = {c["name"]: c for c in table["columns"]}

    assert columns["title"]["type"] == "NVARCHAR2"
    assert columns["title"]["size"] == 100
    assert columns["description"]["type"] == "NVARCHAR2"


def test_gbase8s_mixed_informix_oracle_types():
    """Test GBase 8s table with mixed Informix and Oracle data types."""
    ddl = """
    CREATE TABLE hybrid_table (
        id SERIAL PRIMARY KEY,
        informix_text TEXT,
        informix_money MONEY(16,2),
        informix_datetime DATETIME YEAR TO SECOND,
        oracle_varchar VARCHAR2(100),
        oracle_number NUMBER(10,2),
        informix_interval INTERVAL DAY TO DAY
    );
    """
    result = DDLParser(ddl).run(group_by_type=True)

    table = result["tables"][0]
    columns = {c["name"]: c for c in table["columns"]}

    # Informix types
    assert columns["id"]["type"] == "SERIAL"
    assert columns["informix_text"]["type"] == "TEXT"
    assert columns["informix_money"]["type"] == "MONEY"
    assert columns["informix_datetime"]["type"] == "DATETIME YEAR TO SECOND"
    assert columns["informix_interval"]["type"] == "INTERVAL DAY TO DAY"

    # Oracle-compatible types
    assert columns["oracle_varchar"]["type"] == "VARCHAR2"
    assert columns["oracle_number"]["type"] == "NUMBER"
