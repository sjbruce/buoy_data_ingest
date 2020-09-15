# CSV Duplicate Test Tool
import os, os.path
import glob
import re
import pandas as pd
import configparser
from datetime import datetime
import dateutil
import click

# take file path and load all matching files. 
# - optional regex filtering of results
# - optional listing of what the selected files would be
# - prioritize command line functionality over configurability
#   - do that later and base it off the command line to be self documenting
# - 

def load_files(selected_files):
    """
    Accepts a list of files and a list of columns to check for duplicates
    By default, all columns are checked for count, 
    list of duplicate values per column are optional 
    as is output to file (don't do that here)
    """
    print(selected_files)

    df_prime = pd.DataFrame()

    for file_path in selected_files:
        print(file_path)
        df_new_file = pd.read_csv(file_path)
        # print(df_new_file.head())
        df_prime = df_prime.append(df_new_file)

    print(df_prime.head())
    print(df_prime.tail())
    return df_prime

def find_duplicates(raw_data_frame, check_columns, display_type, ignore_list):
    print(raw_data_frame, check_columns, display_type, ignore_list)
    return

def output(duplicates, sink, destination_path, destination_format):
    """ 
    All final displays go through here 
    default is assumed to be print to stdout 
    file output is optional as is format
    """
    print(duplicates, sink, destination_path, destination_format)
    return


def get_path(source_path, regex_string):
    """ 
    Fetches the path from the os and applies any 
    regular expression filter to the output list 
    of files from the os.
    Default all files os returns are added.

    Use glob module
    """
    selected_files = glob.glob(source_path)
    print(source_path, regex_string)
    print(selected_files)
    return selected_files

@click.command()
@click.argument('source_path', required=True, type=click.Path())
@click.option('--regex_string', default=None, help='')
@click.option('--check_columns', default='*', help='')
@click.option('--display_type', default='COUNT', help='')
@click.option('--ignore_list', default=None, help='')
@click.option('--sink', default='stdout', help='')
@click.option('--destination_path', default=None, help='')
@click.option('--destination_format', default=None, help='')
def main(source_path, regex_string, check_columns, display_type, ignore_list, sink, destination_path, destination_format):
    selected_files = get_path(source_path, regex_string)
    raw_data_frame = load_files(selected_files)
    duplicates = find_duplicates(raw_data_frame, check_columns, display_type, ignore_list)
    output_result = output(duplicates, sink, destination_path, destination_format)
    pass


if __name__ == '__main__':
    main()