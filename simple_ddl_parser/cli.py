import argparse
import os
import sys
from simple_ddl_parser import parse_from_file
from simple_ddl_parser.output.common import output_modes

import pprint


def version(**kwargs):
    return "0.2.0"


def cli():
    sdb_cli = argparse.ArgumentParser(description="Simple DDL Parser")

    sdb_cli.add_argument(
        "ddl_file_path", type=str, help="The path to ddl file to parse"
    )

    sdb_cli.add_argument(
        "-t",
        "--target",
        type=str,
        default="schemas",
        help="Target path to save parse results in .json files",
    )

    sdb_cli.add_argument("-v", action="store_true", default=False, help="Verbose mode")

    sdb_cli.add_argument(
        "--no-dump",
        action="store_true",
        default=False,
        help="Parse without saving to the file. Only print result to the console.",
    )
    sdb_cli.add_argument(
        "-o",
        "--output-mode",
        default="sql",
        help=f"Output mode that will be used to format result. Possible variants: {output_modes}",
    )
    return sdb_cli


def run_for_file(args):
    print(f"Start parsing file {args.ddl_file_path} \n")
    result = parse_from_file(
        args.ddl_file_path,
        dump=not args.no_dump,
        dump_path=args.target,
        output_mode=args.output_mode,
    )

    print(f"File with result was saved to >> {args.target} folder")

    if args.v or args.no_dump:
        pprint.pprint(result)


def correct_extension(file_name: str) -> bool:
    ext = ["ddl", "sql", "hql", ""]
    split_name = file_name.split(".")
    if len(split_name) >= 2:
        ext_file = split_name[1]
        if ext_file in ext:
            return True
    return False


def main():
    sdb_cli = cli()
    args = sdb_cli.parse_args()
    type(args)
    if not os.path.exists(args.ddl_file_path):
        print("The file path specified does not exist")
        sys.exit()
    if os.path.isfile(args.ddl_file_path):
        run_for_file(args)
    else:
        files = [
            os.path.join(args.ddl_file_path, file_name)
            for file_name in os.listdir(args.ddl_file_path)
            if correct_extension(file_name)
        ]
        for file_path in files:
            args.ddl_file_path = file_path
            run_for_file(args)
