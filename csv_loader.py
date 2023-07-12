import argparse
import pandas

from argparse import ArgumentParser, Namespace
from collections.abc import Sequence
from csv import DictReader
from typing import Any
from pandas.api.extensions import no_default


class _OperactionAction(argparse.Action):
    def __init__(self, option_strings: Sequence[str], dest: str, **kwargs) -> None:
        super().__init__(option_strings, dest, **kwargs)
    def __call__(self, parser: ArgumentParser, namespace: Namespace, values: str | Sequence[Any] | None, option_string: str | None = None) -> None:
        if type(values) is str:
            values = [option_string]
        
        operation_name = option_string.strip("-")
        operation = (operation_name, *values)

        if getattr(namespace, self.dest) is None:
            setattr(namespace, self.dest, [])

        getattr(namespace, self.dest).append(operation)

arg_parser = argparse.ArgumentParser(description="Imports a CSV file into the transaction database.")
arg_parser.add_argument("csv", help="Path to the CSV file to import.")
arg_parser.add_argument("--db", default="finances.db", help="Path to the database to insert into.")
arg_parser.add_argument("--colname", "--colnames", action="extend", nargs="*", help="The names of the columns. If none are provided, the first row will be used.")

_col_ops_grp = arg_parser.add_argument_group("Column Operations")
_col_ops_grp.add_argument("--rename",  action=_OperactionAction, dest="opers", nargs=2, metavar=("OLD NAME", "NEW NAME"), help="Renames a column")
_col_ops_grp.add_argument("--combine", action=_OperactionAction, dest="opers", nargs=3, metavar=("COLUMN 1", "COLUMN 2", "NEW NAME"))

if __name__ == "__main__":
    args = arg_parser.parse_args()

    with open(args.csv) as csv_file_handle:
        csv_data = pandas.read_csv(csv_file_handle, names=args.colname or no_default, na_values=[0])
        csv_data.fillna(0, inplace=True)

        # Execute operations on the data
        if args.opers is not None:
            for (op_name, *args) in args.opers:
                if op_name == "rename":
                    csv_data.rename(columns={args[0]: args[1]}, inplace=True)
                elif op_name == "combine":
                    csv_data[args[2]] = csv_data[args[0]] + csv_data[args[1]]
                    csv_data.drop(columns=args[0:2], inplace=True)

        print(csv_data)