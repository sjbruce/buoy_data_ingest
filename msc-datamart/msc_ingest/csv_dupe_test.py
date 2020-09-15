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

def load_files(selected_files, ignore_list):
    """
    Accepts a list of files and a list of columns to check for duplicates
    By default, all columns are checked for count, 
    list of duplicate values per column are optional 
    as is output to file (don't do that here)
    """
    print("Loadings Selected Files...")

    df_prime = pd.DataFrame()

    for file_path in selected_files:
        # Check files against ignore list and add if not contained therein
        print("Adding: %s ..." % (file_path))
        df_new_file = pd.read_csv(file_path)
        print("# of File Records: %s" % (len(df_new_file.index)))
        df_prime = df_prime.append(df_new_file)

    print("Total records from %d selected files: %d" % (len(selected_files), len(df_prime.index)))
    return df_prime

def find_duplicates(raw_data_frame, check_columns):
    # print("Raw Data Frame: %s, Check Columns: %s, Display Type: %s, Ignore List: %s" % (raw_data_frame, check_columns, display_type, ignore_list))

    if check_columns:
        column_list = check_columns.split(',')
        results = raw_data_frame.duplicated(subset=column_list)
    else:
        results = raw_data_frame.duplicated()

    # print("Find Duplicates:")
    # print("Results: \n%s" % (results))
    return results

def output(duplicates, display_type, sink, destination_path, destination_format):
    """ 
    All final displays go through here 
    default is assumed to be print to stdout 
    file output is optional as is format
    """
    duplicates_found = len(duplicates.index)

    if sink == 'stdout':
        print("Duplicates Found: %d" % (duplicates_found))

        if duplicates_found:
            print("Displaying Top 25 Results:")
            print(duplicates.head(25))
        else:
            print("No duplicate records detected based on criteria.")

    elif sink == 'file':
        pass

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
    print("Source Path: %s, Reg Ex: %s" % (source_path, regex_string))
    print("Files Found: %d" % (len(selected_files)))

    if regex_string:
        # apply filter to list of files
        pass

    return selected_files

@click.command()
@click.argument('source_path', required=True, type=click.Path())
@click.option('--regex_string', default=None, help='')
@click.option('--check_columns', default=None, help='')
@click.option('--display_type', default='COUNT', help='')
@click.option('--ignore_list', default=None, help='')
@click.option('--sink', default='stdout', help='')
@click.option('--destination_path', default=None, help='')
@click.option('--destination_format', default=None, help='')
def main(source_path, regex_string, check_columns, display_type, ignore_list, sink, destination_path, destination_format):
    selected_files = get_path(source_path, regex_string)
    raw_data_frame = load_files(selected_files, ignore_list)
    duplicates = find_duplicates(raw_data_frame, check_columns)
    output(raw_data_frame[duplicates], display_type, sink, destination_path, destination_format)

if __name__ == '__main__':
    main()