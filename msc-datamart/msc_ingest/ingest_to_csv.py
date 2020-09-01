'''
Script that is run by a sarracenia plugin when new buoy data is received, 
inserts parsed XML into flat files for ingestion by ERDDAP

See docs at https://dd4.weather.gc.ca/observations/doc/
'''

# TODO could auto-create table here

import os
import click
import pandas as pd
from datetime import datetime
from msc_ingest.parse_xml import buoy_xml_to_json

import configparser
import logging

logging.basicConfig(filename='ingest_to_csv.log', level=logging.DEBUG)
logger = logging.getLogger(__name__)

config = configparser.ConfigParser()
config.read('msc_ingest/ingest_to_csv.conf')

buoy_filename = config['DEFAULT']['buoy_list']
output_dir = config['DEFAULT']['output_dir']
column_headers = config['DEFAULT']['column_headers'].split(',')

# file & date/time formats
dt_format = config['DEFAULT']['datetime_format']
file_name_format = config['DEFAULT']['filename_format'] # station id and date
filename = None
full_path = None

def insert_into_csv(line):
    '''
    inserts dictionary into csv file
    '''
    global filename, full_path
    
    sampling_time = datetime.strptime(line['sampling_time'], '%Y-%m-%dT%H:%M:%S.%fZ')

    filename = file_name_format % (line['wmo_synop_id'], sampling_time.strftime(dt_format))
    full_path = '%s%s' % (output_dir, filename)

    if os.path.isfile(full_path):
        buoy_data = pd.read_csv(full_path)
        buoy_data.append(line)
    else:
        buoy_data = pd.DataFrame(data=line, index=[0], columns=column_headers)

    return buoy_data


def remove_and_log_uninsertable_keys(line):
    '''Remove and report any fields that are in the XML but arent in the database
       This shouldnt be needed, but just in case a new field is added at the source before
       we have a chance to add the column to the DB
    '''

    for k in list(line.keys()):
        if k not in column_headers:
            print(f"Cannot insert {k}")
            del line[k]


def read_buoys_list(filename):
    file = open(filename)
    buoys_list = file.read().strip().split('\n')
    file.close()
    return buoys_list


def ingest_buoy_xml_file(filename):
    '''
    Insert buoy data in XML format into a Pandas DataFrame to be written out to
    a CSV file
    '''

    this_dir = os.path.dirname(os.path.realpath(__file__))

    buoys_list = read_buoys_list(this_dir + '/' + buoy_filename)

    # get a flat dictionary from the source XML
    buoy_record = buoy_xml_to_json(filename)

    if buoy_record['wmo_synop_id'] not in buoys_list:
        print(
            f"Buoy {buoy_record['wmo_synop_id']} not in: {' '.join(buoys_list)}  ")
        return

    # remove and print extra fields that our table doesn't have (yet)
    # probably not necessary if we keep an eye on the mailing lists
    remove_and_log_uninsertable_keys(buoy_record)

    # insert the record
    return insert_into_csv(buoy_record)


@click.command()
@click.argument('filename', type=click.Path(exists=True))
def main(filename):

    buoy_data = ingest_buoy_xml_file(filename)

    if not buoy_data.empty:
        buoy_data.to_csv(path_or_buf=full_path)
        print(f'{buoy_data.size} rows update or inserted in directory ')


if __name__ == '__main__':
    main()
