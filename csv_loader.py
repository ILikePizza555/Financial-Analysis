import argparse
import pandas

from argparse import ArgumentParser, Namespace
from collections.abc import Sequence
from enum import Enum
from typing import Any, Callable, TypeVar
from pandas import DataFrame
from pandas.api.extensions import no_default

def read_csv_dataframe(file_path: str, 
             parse_dates: bool | list | dict = True,
             names: list | None = None,
             skipinitialspace = True,
             fillna_value = 0,
             **read_csv_args) -> DataFrame:
    """Wrapper around pandas read_csv to provide better defaults"""
    dataframe: DataFrame = pandas.read_csv(file_path, parse_dates=parse_dates, names=names, skipinitialspace=skipinitialspace, **read_csv_args)
    dataframe.fillna(fillna_value, inplace=True)
    return dataframe

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

class _OutputActions(Enum):
    PRINT = 1
    LONG_PRINT = 2
    CSV = 3

arg_parser = argparse.ArgumentParser(description="Imports a CSV file into the transaction database.")
arg_parser.add_argument("csv", help="Path to the CSV file to import.")
arg_parser.add_argument("--colname", "--colnames", action="extend", nargs="*", help="The names of the columns. If none are provided, the first row will be used.")

_output_actions_grp = arg_parser.add_argument_group("Output Actions")
_output_actions_grp.add_argument("--print",      action="store_const", dest="output", const=_OutputActions.PRINT)
_output_actions_grp.add_argument("--long-print", action="store_const", dest="output", const=_OutputActions.LONG_PRINT, help="Print out the entirety of the data.")
_output_actions_grp.add_argument("--csv-print",  action="store_const", dest="output", const=_OutputActions.CSV, help="Print out the data as a CSV.")
_output_actions_grp.add_argument("--db", nargs=1, metavar="PATH", help="Path to the database to write the data to.")
arg_parser.set_defaults(output=_OutputActions.PRINT)

_col_ops_grp = arg_parser.add_argument_group("Column Operations")
_col_ops_grp.add_argument("--rename",  action=_OperationAction, dest="opers", op_func=rename_operation, nargs=2, metavar=("OLD NAME", "NEW NAME"), help="Renames a column")
_col_ops_grp.add_argument("--combine", action=_OperationAction, dest="opers", op_func=combine_operation, nargs=3, metavar=("COLUMN 1", "COLUMN 2", "NEW NAME"))
_col_ops_grp.add_argument("--drop", action=_OperationAction, dest="opers", op_func=drop_operation, nargs="*", metavar="COLUMN")

def _main():
    args = arg_parser.parse_args()

    csv_data = read_csv_dataframe(args.csv, names=args.colname or None)

    # Execute operations on the data
    if args.opers is not None:
        for op in args.opers:
            op(csv_data)

    # Output
    match args.output:
        case _OutputActions.PRINT:
            print(csv_data)
            return
        case _OutputActions.LONG_PRINT:
            print(csv_data.to_string())
            return
        case _OutputActions.CSV:
            print(csv_data.to_csv())
            return
        
        

if __name__ == "__main__":
    _main()