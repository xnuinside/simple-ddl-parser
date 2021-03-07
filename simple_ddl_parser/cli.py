import argparse
import os
import sys
from simple_ddl_parser import parse_from_file
import pprint


def version(**kwargs):
    return '0.2.0'


def cli():
    sdb_cli = argparse.ArgumentParser(description='Simple DDL Parser')

    sdb_cli.add_argument('ddl_file_path',
                        type=str,
                        help='The path to ddl file to parse')
    
    sdb_cli.add_argument('-v',
                        action='store_true',
                        default=False,
                        help='Verbose mode')
    
    sdb_cli.add_argument('--no-dump',
                        action='store_true',
                        default=False,
                        help='Parse without saving to the file. Only print result to the console.')
    return sdb_cli


def main():
    sdb_cli = cli()
    args = sdb_cli.parse_args()
    
    input_path = args.ddl_file_path
    target_folder ='schemas'
    if not os.path.isfile(input_path):
        print('The file path specified does not exist or it is a folder')
        sys.exit()
    
    print(f"Start parsing file {input_path} \n")
    result = parse_from_file(input_path, dump=not args.no_dump)
    
    print(f"File with result was saved to {target_folder} folder")
    
    if args.v or args.no_dump:
        pprint.pprint(result)