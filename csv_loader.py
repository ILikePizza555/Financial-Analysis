import argparse
import pandas

from argparse import ArgumentParser, Namespace
from collections.abc import Sequence
from csv import DictReader
from typing import Any, Callable, TypeVar
from pandas import DataFrame
from pandas.api.extensions import no_default

OpFactory = Callable[..., Callable[[DataFrame], None]]

def rename_operation(old_name, new_name):
    def op(data: DataFrame):
        data.rename(columns={old_name: new_name}, inplace=True)
    return op

def combine_operation(col1, col2, new_name):
    def op(data: DataFrame):
        data[new_name] = data[col1] + data[col2]
        data.drop(columns=[col1, col2], inplace=True)
    return op

def drop_operation(columns):
    def op(data: DataFrame):
        data.drop(columns=columns, inplace=True)
    return op

class _OperationAction(argparse.Action):
    def __init__(self, option_strings: Sequence[str], dest: str, op_func: OpFactory, **kwargs) -> None:
        if op_func is None:
            raise TypeError("op_func cannot be None")

        self.op_func = op_func
        super().__init__(option_strings, dest, **kwargs)
    def __call__(self, parser: ArgumentParser, namespace: Namespace, values: str | Sequence[Any] | None, option_string: str | None = None) -> None:
        if type(values) is str:
            values = [option_string]
        
        operation = self.op_func(*values)

        if getattr(namespace, self.dest) is None:
            setattr(namespace, self.dest, [])

        getattr(namespace, self.dest).append(operation)

arg_parser = argparse.ArgumentParser(description="Imports a CSV file into the transaction database.")
arg_parser.add_argument("csv", help="Path to the CSV file to import.")
arg_parser.add_argument("--db", default="finances.db", help="Path to the database to insert into.")
arg_parser.add_argument("--colname", "--colnames", action="extend", nargs="*", help="The names of the columns. If none are provided, the first row will be used.")

_col_ops_grp = arg_parser.add_argument_group("Column Operations")
_col_ops_grp.add_argument("--rename",  action=_OperationAction, dest="opers", op_func=rename_operation, nargs=2, metavar=("OLD NAME", "NEW NAME"), help="Renames a column")
_col_ops_grp.add_argument("--combine", action=_OperationAction, dest="opers", op_func=combine_operation, nargs=3, metavar=("COLUMN 1", "COLUMN 2", "NEW NAME"))
_col_ops_grp.add_argument("--drop", action=_OperationAction, dest="opers", op_func=drop_operation, nargs="*", metavar="COLUMN")

def _main():
    args = arg_parser.parse_args()

    with open(args.csv) as csv_file_handle:
        csv_data = pandas.read_csv(csv_file_handle, names=args.colname or no_default, na_values=[0])
        csv_data.fillna(0, inplace=True)

        # Execute operations on the data
        if args.opers is not None:
            for op in args.opers:
                op(csv_data)

        print(csv_data)

if __name__ == "__main__":
    _main()