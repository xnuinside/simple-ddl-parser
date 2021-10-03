from simple_ddl_parser import DDLParser

def test_simple_on_update():
	ddl = """
	CREATE TABLE t1 (
	  ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
	  dt DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
	);
	"""
	result = DDLParser(ddl).run(group_by_type=True)
	expected = {'tables': [
		{'columns': [{'name': 'ts', 'type': 'TIMESTAMP', 'size': None, 'references': None,
		              'unique': False, 'nullable': True, 'default': 'CURRENT_TIMESTAMP', 'check': None,
		              'on_update': 'CURRENT_TIMESTAMP'}, {'name': 'dt', 'type': 'DATETIME', 'size': None,
		                                                  'references': None, 'unique': False, 'nullable': True,
		                                                  'default': 'CURRENT_TIMESTAMP', 'check': None,
		                                                  'on_update': 'CURRENT_TIMESTAMP'}], 'primary_key': [],
		 'alter': {}, 'checks': [], 'index': [], 'partitioned_by': [], 'tablespace': None, 'schema': None,
		 'table_name': 't1'}], 'types': [], 'sequences': [], 'domains': [], 'schemas': [],
'ddl_properties': [],
}
	assert expected == result


def test_on_update_with_fcall():
	ddl = """
	create table test(
	  `id` bigint not null,
	  `updated_at` timestamp(3) not null default current_timestamp(3) on update current_timestamp(3),
	  primary key (id)
	);
	"""
	result = DDLParser(ddl).run(group_by_type=True)
	expcted ={'domains': [],
 'schemas': [],
 'sequences': [],
 'tables': [{'alter': {},
             'checks': [],
             'columns': [{'check': None,
                          'default': None,
                          'name': '`id`',
                          'nullable': False,
                          'references': None,
                          'size': None,
                          'type': 'bigint',
                          'unique': False},
                         {'check': None,
                          'default': 'current_timestamp(3)',
                          'name': '`updated_at`',
                          'nullable': False,
                          'on_update': 'current_timestamp(3)',
                          'references': None,
                          'size': 3,
                          'type': 'timestamp',
                          'unique': False}],
             'index': [],
             'partitioned_by': [],
             'primary_key': ['id'],
             'schema': None,
             'table_name': 'test',
             'tablespace': None, }],
              'ddl_properties': [],
 'types': []}
	assert expcted == result

